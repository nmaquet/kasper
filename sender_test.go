package kasper

import (
	"testing"
	"reflect"
	"github.com/Shopify/sarama"
)

func TestSender_Send_OneMessage(t *testing.T) {
	pp := &partitionProcessor{
		topicProcessor: &TopicProcessor{
			config: &TopicProcessorConfig{
				TopicSerdes: map[Topic]TopicSerde{
					"hello": {
						KeySerde:   NewStringSerde(),
						ValueSerde: NewStringSerde(),
					},
				},
			},
		},
	}
	in := &IncomingMessage{}
	sender := newSender(pp, in)
	out := OutgoingMessage{
		Topic:     "hello",
		Partition: 6,
		Key:       "AAA",
		Value:     "BBB",
	}
	sender.Send(out)
	if len(sender.producerMessages) != 1 {
		t.Fail()
	}
	expected := &sarama.ProducerMessage{
		Topic:     "hello",
		Key:       sarama.ByteEncoder([]byte{65, 65, 65}),
		Value:     sarama.ByteEncoder([]byte{66, 66, 66}),
		Partition: 6,
		Metadata:  in,
	}
	actual := sender.producerMessages[0]
	if ! reflect.DeepEqual(actual, expected) {
		t.Error(actual)
	}
}