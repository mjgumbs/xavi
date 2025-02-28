package xavi

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestPublishAndListen tests that publishing a message results in the message being delivered and acknowledged.
func TestPublishAndListen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a new client instance.
	client, err := New(ctx)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	// Delay client shutdown until after message processing.
	defer client.Close()

	// Add a subscriber for subject "event.test".
	if err := client.AddSubscriber(ctx, "test", "event.test"); err != nil {
		t.Fatalf("failed to add subscriber: %v", err)
	}

	// Use channels and a wait group to synchronize ack processing.
	msgCh := make(chan []byte, 1)
	done := make(chan struct{})
	var ackErr error
	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(m Message) {
		defer wg.Done()
		// Send the message data to msgCh for verification.
		msgCh <- m.Data()
		// Acknowledge the message.
		ackErr = m.Ack()
		// Signal that processing is complete.
		close(done)
	}

	// Start listening for messages with the consumer name "test".
	stop, err := client.Listen(ctx, "test", handler)
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	// Ensure we stop the consumer only after processing.
	defer stop()

	// Publish a test message on subject "event.test".
	testMsg := []byte("hello")
	if err := client.Publish(ctx, "event.test", testMsg); err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Wait for the message to be received.
	select {
	case msg := <-msgCh:
		if string(msg) != "hello" {
			t.Errorf("unexpected message: got %s, want %s", msg, "hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	// Wait for the handler to finish processing (including Ack).
	wg.Wait()

	// Check if ack returned an error.
	if ackErr != nil {
		t.Fatalf("failed to ack message: %v", ackErr)
	}
}
