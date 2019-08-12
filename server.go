package arpc

import (
	"log"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type Server struct {
	queueName string
	channel   *amqp.Channel
	handler   func(msg []byte) []byte
}

func defaultServer() *Server {
	return &Server{}
}

func NewServer(queueName string, connection *amqp.Connection, handler func(msg []byte) []byte) (*Server, error) {
	channel, err := connection.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "error create channel")
	}

	err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	s := defaultServer()
	s.queueName = queueName
	s.channel = channel
	s.handler = handler

	return s, nil
}

func (s *Server) ListenAndServe() error {
	log.Printf(">>> Start the server...\n")

	msgs, err := s.channel.Consume(
		s.queueName,
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return errors.Wrap(err, "error consume queue")
	}

	for d := range msgs {
		go func(dd amqp.Delivery) {
			log.Println(">>> Receiving msg: ", string(dd.Body))
			err = s.channel.Publish(
				"",         // exchange
				dd.ReplyTo, // routing key
				false,      // mandatory
				false,      // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: dd.CorrelationId,
					Body:          s.handler(dd.Body),
				})
			failOnError(err, "Failed to publish a message")
			dd.Ack(false)
		}(d)
	}

	return nil
}
