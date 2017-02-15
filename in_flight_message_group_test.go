package kasper

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

func TestInFlightMessageGroup_allAcksAreTrue_emptyGroup(t *testing.T) {
	g := inFlightMessageGroup{
		incomingMessage:  &IncomingMessage{},
		inFlightMessages: []*inFlightMessage{},
	}
	assert.True(t, g.allAcksAreTrue())
}

func TestInFlightMessageGroup_allAcksAreTrue_noAcks(t *testing.T) {
	g := inFlightMessageGroup{
		incomingMessage: &IncomingMessage{},
		inFlightMessages: []*inFlightMessage{
			{
				msg: &sarama.ProducerMessage{},
				ack: false,
			},
		},
	}
	assert.False(t, g.allAcksAreTrue())
}

func TestInFlightMessageGroup_allAcksAreTrue_nilGroup(t *testing.T) {
	g := inFlightMessageGroup{
		incomingMessage:  &IncomingMessage{},
		inFlightMessages: nil,
	}
	assert.True(t, g.allAcksAreTrue())
}

func TestInFlightMessageGroup_allAcksAreTrue_oneAck(t *testing.T) {
	g := inFlightMessageGroup{
		incomingMessage: &IncomingMessage{},
		inFlightMessages: []*inFlightMessage{
			{
				msg: &sarama.ProducerMessage{},
				ack: true,
			},
		},
	}
	assert.True(t, g.allAcksAreTrue())
}

func TestInFlightMessageGroup_allAcksAreTrue_mixed(t *testing.T) {
	g := inFlightMessageGroup{
		incomingMessage: &IncomingMessage{},
		inFlightMessages: []*inFlightMessage{
			{
				msg: &sarama.ProducerMessage{},
				ack: true,
			},
			{
				msg: &sarama.ProducerMessage{},
				ack: false,
			},
		},
	}
	assert.False(t, g.allAcksAreTrue())
}
