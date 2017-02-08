package kasper

import "time"

type IncomingMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       interface{}
	Value     interface{}
	Timestamp time.Time
}

type OutgoingMessage struct {
	// TODO
}
