package gq

import "time"

// Message minimal message definition
type Message struct {
	Payload []byte
}

// ConsumerMessage message for a consumer
type ConsumerMessage struct {
	Message
	Id int64
}

// Receipt to commit
type Receipt struct {
	Id      int64
	Success bool
}

// MQ Implemented message queue interface
type MQ interface {
	// Initialization
	Create() error
	// Destruction
	Destroy() error
	// Simple send message
	Publish(messages []*Message) error
	// Request a batch of messages
	ConsumeBatch(size int) ([]*ConsumerMessage, error)
	// Way to consume a stream of messages
	Stream(size int, messages chan []*ConsumerMessage, pause time.Duration)
	// End the stream
	StopConsumer()
	// Commit that a messages has been processed
	Commit(recipts []*Receipt) error
}
