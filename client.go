package arpc

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

const (
	defaultGracefulCloseTimeout = 10000000000 // 10 seconds
)

var (
	// ErrClientClosed error due to client is already closed
	ErrClientClosed = errors.New("client already closed")
)

// Client a request/reply client.
type Client struct {
	channel *amqp.Channel

	// hold pairs of correlationID and a channel to receive result
	receivers   map[string]chan []byte
	muReceivers *sync.RWMutex

	closed   bool
	muClosed *sync.RWMutex

	queueResponse        string
	gracefulCloseTimeout time.Duration
	log                  func(string)
}

// defaultClient set default value of a client
func defaultClient() *Client {
	return &Client{
		receivers:            make(map[string]chan []byte, 1000),
		muReceivers:          &sync.RWMutex{},
		closed:               false,
		muClosed:             &sync.RWMutex{},
		gracefulCloseTimeout: defaultGracefulCloseTimeout,
		log: func(msg string) {
			log.Println(msg)
		},
	}
}

// WithLog return option to set log
func WithLog(f func(string)) func(*Client) {
	return func(c *Client) {
		c.log = f
	}
}

// WithCloseTimeout set timeout to gracefully close the client
func WithCloseTimeout(t time.Duration) func(*Client) {
	return func(c *Client) {
		c.gracefulCloseTimeout = t
	}
}

// NewClient initiate a new client that can be used to send request.
// Only use amqp.Connection to create a new channel.
func NewClient(conn *amqp.Connection, opts ...func(*Client)) (*Client, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "error create channel")
	}

	c := defaultClient()
	c.channel = channel
	for _, opt := range opts {
		opt(c)
	}

	if err = c.setQueueResponse(); err != nil {
		return nil, errors.Wrap(err, "error create queue response")
	}

	go c.listen()

	return c, nil
}

// Close stopping consumer and close all resources
func (c *Client) Close() {
	c.muClosed.Lock()
	c.closed = true
	c.muClosed.Unlock()

	if c.isReceiverEmpty() {
		c.channel.Close()
		c.log("immediate close")
		return
	}

	ticker := time.NewTicker(c.gracefulCloseTimeout)
	defer ticker.Stop()

	for !c.isReceiverEmpty() {
		select {
		case <-ticker.C:
			c.channel.Close()
			c.log("close after timeout")
			return
		default:
			c.log("receivers is not yet empty, still waiting...")
			time.Sleep(1 * time.Second)
		}
	}

	c.channel.Close()
	c.log("close after waiting")
}

// Send send request to spesific service
func (c *Client) Send(ctx context.Context, dest string, msg []byte) ([]byte, error) {
	if c.isClosed() {
		return nil, ErrClientClosed
	}

	correlationID := UUID(false)
	chResult := make(chan []byte)
	c.registerReceiver(correlationID, chResult)

	err := c.channel.Publish(
		"",    // exchange
		dest,  // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: correlationID,
			ReplyTo:       c.queueResponse,
			Body:          msg,
		})
	if err != nil {
		return nil, err
	}

	result := <-chResult

	return result, nil
}

// setQueueResponse create a new queue and set that queue as queue for receiving response.
func (c *Client) setQueueResponse() error {
	q, err := c.channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return err
	}
	c.queueResponse = q.Name
	return nil
}

// registerReceiver set pair of id and ch (receiver channel) in the receivers map.
func (c *Client) registerReceiver(id string, ch chan []byte) {
	c.muReceivers.Lock()
	defer c.muReceivers.Unlock()

	c.receivers[id] = ch
}

// sendResult send result to its requestor.
func (c *Client) sendResult(id string, result []byte) {
	c.muReceivers.Lock()
	// copy value of this receiver, so we can delete this slot and unlock mutex faster
	ch := c.receivers[id]
	delete(c.receivers, id)
	c.muReceivers.Unlock()

	ch <- result
}

// listen listen for the response for all requests.
func (c *Client) listen() {
	msgs, err := c.channel.Consume(
		c.queueResponse, // queue name
		"",              // consumer
		true,            // auto-ack
		true,            // make sure this treated as a sole consumer
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	failOnError(err, "listen - Failed to register a consumer")

	for d := range msgs {
		c.sendResult(d.CorrelationId, d.Body)
	}
}

func (c *Client) isReceiverEmpty() bool {
	c.muReceivers.RLock()
	defer c.muReceivers.RUnlock()

	return len(c.receivers) < 1
}

func (c *Client) isClosed() bool {
	c.muClosed.RLock()
	defer c.muClosed.RUnlock()

	return c.closed
}
