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
	sender := newSender(f.pp)
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
		Metadata:  nil,
	}
	actual := sender.producerMessages[0]
	assert.Equal(t, expected, actual)
}

func TestSender_Send_TwoMessages(t *testing.T) {
	f := newFixture()
	sender := newSender(f.pp)
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
			Metadata:  nil,
		},
		{
			Topic:     "hello",
			Key:       sarama.ByteEncoder([]byte{67, 67, 67}),
			Value:     sarama.ByteEncoder([]byte{68, 68, 68}),
			Partition: 7,
			Metadata:  nil,
		},
	}
	actual := sender.producerMessages
	assert.Equal(t, expected, actual)
}

func TestSender_Send_MissingSerde(t *testing.T) {
	f := newFixture()
	sender := newSender(f.pp)
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

func BenchmarkSender_Send(b *testing.B) {
	f := newFixture()
	sender := newSender(f.pp)
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
