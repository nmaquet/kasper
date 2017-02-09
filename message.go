package kasper

import "time"

type IncomingMessage struct {
	Topic     Topic
	Partition Partition
	Offset    Offset
	Key       interface{}
	Value     interface{}
	Timestamp time.Time
}

type OutgoingMessage struct {
	Topic     Topic
	Partition Partition
	Key       interface{}
	Value     interface{}
}
