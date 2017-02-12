package kasper

import "time"

type IncomingMessage struct {
	Topic     string
	Partition int
	Offset    int64
	Key       interface{}
	Value     interface{}
	Timestamp time.Time
}

type OutgoingMessage struct {
	Topic     string
	Partition int
	Key       interface{}
	Value     interface{}
}
