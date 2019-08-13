package arpc

import (
	"log"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// Server an ARPC server
type Server struct {
	connection *amqp.Connection
	handlers   map[string]func(msg []byte) []byte
}

func defaultServer() *Server {
	return &Server{
		handlers: make(map[string]func(msg []byte) []byte),
	}
}

// NewServer initiate a new server
func NewServer(connection *amqp.Connection) (*Server, error) {
	s := defaultServer()
	s.connection = connection

	return s, nil
}

// AddCommand register command
func (s *Server) AddCommand(key string, f func(msg []byte) []byte) {
	s.handlers[key] = f
}

// ListenAndServe start listen and serve
func (s *Server) ListenAndServe() error {
	log.Printf(">>> Start the server...\n")
	if len(s.handlers) == 0 {
		return errors.New("no handlers defined")
	}

	for k, v := range s.handlers {
		go s.listen(k, v)
	}

	return nil
}

func (s *Server) listen(key string, f func([]byte) []byte) {
	channel, err := s.connection.Channel()
	if err != nil {
		failOnError(err, "error create channel")
	}

	err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		failOnError(err, "error set qos")
	}

	msgs, err := channel.Consume(
		key,
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		failOnError(err, "error consume queue")
	}

	for d := range msgs {
		go func(dd amqp.Delivery) {
			err = channel.Publish(
				"",         // exchange
				dd.ReplyTo, // routing key
				false,      // mandatory
				false,      // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: dd.CorrelationId,
					Body:          f(dd.Body),
				})
			failOnError(err, "failed to publish a message")
			dd.Ack(false)
		}(d)
	}
}
