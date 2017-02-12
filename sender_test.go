package kasper

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

type fixture struct {
	pp *partitionProcessor
	in *IncomingMessage
}

func newFixture() *fixture {
	return &fixture{
		&partitionProcessor{
			topicProcessor: &TopicProcessor{
				config: &TopicProcessorConfig{
					TopicSerdes: map[string]TopicSerde{
						"hello": {
							KeySerde:   NewStringSerde(),
							ValueSerde: NewStringSerde(),
						},
					},
				},
			},
		},
		&IncomingMessage{},
	}
}

func TestSender_Send_OneMessage(t *testing.T) {
	f := newFixture()
	sender := newSender(f.pp, f.in)
	out := OutgoingMessage{
		Topic:     "hello",
		Partition: 6,
		Key:       "AAA",
		Value:     "BBB",
	}
	sender.Send(out)
	if len(sender.producerMessages) != 1 {
		t.Errorf("Expected 1 message but got %d", len(sender.producerMessages))
	}
	expected := &sarama.ProducerMessage{
		Topic:     "hello",
		Key:       sarama.ByteEncoder([]byte{65, 65, 65}),
		Value:     sarama.ByteEncoder([]byte{66, 66, 66}),
		Partition: 6,
		Metadata:  f.in,
	}
	actual := sender.producerMessages[0]
	assert.Equal(t, expected, actual)
}

func TestSender_Send_TwoMessages(t *testing.T) {
	f := newFixture()
	sender := newSender(f.pp, f.in)
	sender.Send(OutgoingMessage{
		Topic:     "hello",
		Partition: 6,
		Key:       "AAA",
		Value:     "BBB",
	})
	sender.Send(OutgoingMessage{
		Topic:     "hello",
		Partition: 7,
		Key:       "CCC",
		Value:     "DDD",
	})
	if len(sender.producerMessages) != 2 {
		t.Fail()
	}
	expected := []*sarama.ProducerMessage{
		{
			Topic:     "hello",
			Key:       sarama.ByteEncoder([]byte{65, 65, 65}),
			Value:     sarama.ByteEncoder([]byte{66, 66, 66}),
			Partition: 6,
			Metadata:  f.in,
		},
		{
			Topic:     "hello",
			Key:       sarama.ByteEncoder([]byte{67, 67, 67}),
			Value:     sarama.ByteEncoder([]byte{68, 68, 68}),
			Partition: 7,
			Metadata:  f.in,
		},
	}
	actual := sender.producerMessages
	assert.Equal(t, expected, actual)
}

func TestSender_Send_MissingSerde(t *testing.T) {
	f := newFixture()
	sender := newSender(f.pp, f.in)
	out := OutgoingMessage{
		Topic:     "unknown",
		Partition: 6,
		Key:       "AAA",
		Value:     "BBB",
	}
	assert.Panics(t, func() {
		sender.Send(out)
	})
}

func TestSender_createInFlightMessageGroup(t *testing.T) {
	f := newFixture()
	sender := newSender(f.pp, f.in)
	sender.Send(OutgoingMessage{
		Topic:     "hello",
		Partition: 6,
		Key:       "AAA",
		Value:     "BBB",
	})
	sender.Send(OutgoingMessage{
		Topic:     "hello",
		Partition: 7,
		Key:       "CCC",
		Value:     "DDD",
	})
	actual := sender.createInFlightMessageGroup(true)
	expected := &inFlightMessageGroup{
		incomingMessage: f.in,
		committed:       true,
		inFlightMessages: []*inFlightMessage{
			{
				msg: &sarama.ProducerMessage{
					Topic:     "hello",
					Key:       sarama.ByteEncoder([]byte{65, 65, 65}),
					Value:     sarama.ByteEncoder([]byte{66, 66, 66}),
					Partition: 6,
					Metadata:  f.in,
				},
				ack: false,
			},
			{
				msg: &sarama.ProducerMessage{
					Topic:     "hello",
					Key:       sarama.ByteEncoder([]byte{67, 67, 67}),
					Value:     sarama.ByteEncoder([]byte{68, 68, 68}),
					Partition: 7,
					Metadata:  f.in,
				},
				ack: false,
			},
		},
	}
	assert.Equal(t, expected, actual)
}

func BenchmarkSender_Send(b *testing.B) {
	f := newFixture()
	sender := newSender(f.pp, f.in)
	for i := 0; i < b.N; i++ {
		out := OutgoingMessage{
			Topic:     "hello",
			Partition: 6,
			Key:       "AAA",
			Value:     "BBB",
		}
		sender.Send(out)
	}
}
