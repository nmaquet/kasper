package kasper

import "time"

// IncomingMessage describes Kafka incoming message
type IncomingMessage struct {
	Topic     string
	Partition int
	Offset    int64
	Key       interface{}
	Value     interface{}
	Timestamp time.Time
}

// OutgoingMessage describes Kafka outgoing message
type OutgoingMessage struct {
	Topic     string
	Partition int
	Key       interface{}
	Value     interface{}
}
