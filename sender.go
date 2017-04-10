package kasper

import (
	"github.com/Shopify/sarama"
)

// Sender describes an interface for sending messages to Kafka topics
type Sender interface {
	Send(OutgoingMessage) // Send message to output topics
}

type sender struct {
	pp               *partitionProcessor
	producerMessages []*sarama.ProducerMessage
}

func newSender(pp *partitionProcessor) *sender {
	return &sender{
		pp,
		[]*sarama.ProducerMessage{},
	}
}

func (sender *sender) Send(msg OutgoingMessage) {
	producerMessage := &sarama.ProducerMessage{
		Topic:     msg.Topic,
		Key:       sarama.ByteEncoder(msg.Key),
		Value:     sarama.ByteEncoder(msg.Value),
		Partition: int32(msg.Partition),
	}
	sender.producerMessages = append(sender.producerMessages, producerMessage)
}
