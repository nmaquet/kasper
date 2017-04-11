package kasper

import (
	"github.com/Shopify/sarama"
)

// Sender describes an interface for sending messages to Kafka topics
type Sender interface {
	Send(msg *sarama.ProducerMessage) // Send message to output topics
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

func (sender *sender) Send(msg *sarama.ProducerMessage) {
	sender.producerMessages = append(sender.producerMessages, msg)
}
