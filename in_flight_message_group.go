package kasper

import "github.com/Shopify/sarama"

type inFlightMessage struct {
	msg *sarama.ProducerMessage
	ack bool
}

type inFlightMessageGroup struct {
	incomingMessage  *IncomingMessage
	inFlightMessages []*inFlightMessage
	committed        bool
}

func (group *inFlightMessageGroup) allAcksAreTrue() bool {
	if group.inFlightMessages == nil {
		return true
	}
	for _, msg := range group.inFlightMessages {
		if !msg.ack {
			return false
		}
	}
	return true
}
