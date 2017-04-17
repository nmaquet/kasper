package kasper

import (
	"github.com/Shopify/sarama"
)

// Sender instances are given to MessageProcessor.Process to send messages to Kafka topics.
// Messages passed to Sender are not sent directly but are collected in an array instead.
// When Process returns, the messages are sent to Kafka and Kasper waits for the configured number of acks.
// When all messages have been successfully produced, Kasper updates the consumer offsets of the input partitions
// and resumes processing.
type Sender interface {
	Send(msg *sarama.ProducerMessage)
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
