// Package xavi provides an embedded NATS server with JetStream support,
// including publishing, subscribing, and message handling functionalities.
package xavi

import (
	"context"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
)

// Options holds configuration settings for the embedded server.
type Options struct {
	// EnableLogging indicates whether logging should be enabled for the server.
	EnableLogging bool
}

// Message represents a generic message interface that supports acknowledgment
// and provides access to the message data.
type Message interface {
	// Ack sends an acknowledgment for the message.
	Ack() error
	// Data returns the payload of the message.
	Data() []byte
}

// wrappedMsg is a concrete implementation of Message that wraps a jetstream.Msg.
type wrappedMsg struct {
	msg jetstream.Msg
}

// Ack acknowledges the underlying jetstream message.
func (m *wrappedMsg) Ack() error {
	return m.msg.Ack()
}

// Data returns the data payload of the underlying jetstream message.
func (m *wrappedMsg) Data() []byte {
	return m.msg.Data()
}

// Client encapsulates a NATS connection, an embedded NATS server, and JetStream context
// along with a JetStream stream for event handling.
type Client struct {
	nc     *nats.Conn          // nc is the active NATS connection.
	ns     *server.Server      // ns is the embedded NATS server instance.
	js     jetstream.JetStream // js provides the JetStream context for messaging.
	stream jetstream.Stream    // stream is the JetStream stream used for event messages.
}

// Publish sends data on the given subject using the JetStream context.
func (c *Client) Publish(ctx context.Context, subject string, data []byte) error {
	// Publish the data to the subject.
	c.js.Publish(ctx, subject, data)
	return nil
}

// AddSubscriber creates or updates a consumer to subscribe to messages on a given event subject.
// 'name' is used for both the consumer name and durable name, ensuring persistence.
func (c *Client) AddSubscriber(ctx context.Context, name, event string) error {
	_, err := c.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          name,
		Durable:       name,
		FilterSubject: event,
	})
	if err != nil {
		return err
	}
	return nil
}

// Listen registers a message handler for a specific consumer.
// It returns a function that can be called to stop listening, along with any error encountered.
func (c *Client) Listen(ctx context.Context, name string, handler func(Message)) (func(), error) {
	// Retrieve the consumer configuration by name from the 'events' stream.
	consumer, err := c.js.Consumer(ctx, "events", name)
	if err != nil {
		return nil, err
	}

	// Start consuming messages using the provided handler.
	consumerCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		// Wrap the received message and pass it to the handler.
		wrapped := &wrappedMsg{msg: msg}
		handler(wrapped)
	})
	if err != nil {
		return nil, err
	}

	// Define the cleanup function to stop the consumer.
	close := func() {
		consumerCtx.Stop()
	}

	return close, nil
}

// Close terminates the NATS connection and shuts down the embedded server.
func (c *Client) Close() {
	c.nc.Close()
	c.ns.Shutdown()
}

// New initializes a new Client by starting an embedded server, creating a NATS connection,
// setting up JetStream, and creating a stream for events.
func New(ctx context.Context) (*Client, error) {
	opts := &Options{}
	// Start the embedded NATS server.
	nc, ns, err := runEmbeddedServer(opts)
	if err != nil {
		return nil, err
	}

	// Create a JetStream context.
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	// Create a stream named "events" that handles subjects prefixed with "event.".
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "events",
		Subjects: []string{"event.*"},
	})
	if err != nil {
		return nil, err
	}

	// Assemble the Client with all the necessary components.
	client := &Client{
		nc:     nc,
		ns:     ns,
		js:     js,
		stream: stream,
	}

	return client, nil
}

// runEmbeddedServer starts an embedded NATS server with the given options,
// sets up logging if enabled, and creates a NATS connection to the server.
func runEmbeddedServer(opts *Options) (*nats.Conn, *server.Server, error) {
	// Configure the server options.
	serverOpts := &server.Options{
		ServerName: "xavi",
		DontListen: true, // Prevents the server from listening on a network port.
		JetStream:  true, // Enables JetStream support.
		StoreDir:   "~/", // Directory for storing JetStream data.
	}

	// Create the embedded NATS server.
	ns, err := server.NewServer(serverOpts)
	if err != nil {
		return nil, nil, err
	}

	// Enable logging if specified in the options.
	if opts.EnableLogging {
		ns.ConfigureLogger()
	}

	// Start the server asynchronously.
	go ns.Start()

	// Wait until the server is ready to accept connections or timeout after 5 seconds.
	if !ns.ReadyForConnections(5 * time.Second) {
		return nil, nil, errors.New("service failed to start")
	}

	// Set up client options to connect to the embedded server.
	clientOptions := []nats.Option{nats.InProcessServer(ns)}

	// Connect to the embedded server using the client options.
	nc, err := nats.Connect(ns.ClientURL(), clientOptions...)
	if err != nil {
		return nil, nil, err
	}

	return nc, ns, nil
}
