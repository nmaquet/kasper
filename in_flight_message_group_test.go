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
		committed:        false,
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
		committed: false,
	}
	assert.False(t, g.allAcksAreTrue())
}

func TestInFlightMessageGroup_allAcksAreTrue_nilGroup(t *testing.T) {
	g := inFlightMessageGroup{
		incomingMessage:  &IncomingMessage{},
		inFlightMessages: nil,
		committed:        false,
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
		committed: false,
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
		committed: false,
	}
	assert.False(t, g.allAcksAreTrue())
}
