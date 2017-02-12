package kasper

import "time"

type IncomingMessage struct {
	Topic     Topic
	Partition int
	Offset    int64
	Key       interface{}
	Value     interface{}
	Timestamp time.Time
}

type OutgoingMessage struct {
	Topic     Topic
	Partition int
	Key       interface{}
	Value     interface{}
}
