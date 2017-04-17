package kasper

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

type fixture struct {
	pp *partitionProcessor
	in *sarama.ConsumerMessage
}

func newFixture() *fixture {
	return &fixture{
		&partitionProcessor{
			topicProcessor: &TopicProcessor{
				config: &Config{},
			},
		},
		&sarama.ConsumerMessage{},
	}
}

func TestSender_Send_OneMessage(t *testing.T) {
	f := newFixture()
	sender := newSender(f.pp)
	out := &sarama.ProducerMessage{
		Topic:     "hello",
		Partition: 6,
		Key:       sarama.ByteEncoder([]byte("AAA")),
		Value:     sarama.ByteEncoder([]byte("BBB")),
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
	sender.Send(&sarama.ProducerMessage{
		Topic:     "hello",
		Partition: 6,
		Key:       sarama.ByteEncoder([]byte("AAA")),
		Value:     sarama.ByteEncoder([]byte("BBB")),
	})
	sender.Send(&sarama.ProducerMessage{
		Topic:     "hello",
		Partition: 7,
		Key:       sarama.ByteEncoder([]byte("CCC")),
		Value:     sarama.ByteEncoder([]byte("DDD")),
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

func BenchmarkSender_Send(b *testing.B) {
	f := newFixture()
	sender := newSender(f.pp)
	for i := 0; i < b.N; i++ {
		out := &sarama.ProducerMessage{
			Topic:     "hello",
			Partition: 6,
			Key:       sarama.ByteEncoder([]byte("AAA")),
			Value:     sarama.ByteEncoder([]byte("BBB")),
		}
		sender.Send(out)
	}
}
