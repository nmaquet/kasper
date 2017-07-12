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

	// Send appends a message to a slice held by the sender instance.
	// These messages are sent in bulk when Process() returns or when Flush() is called.
	Send(msg *sarama.ProducerMessage)

	// Flush immediately sends all messages held in the sender slice in bulk, and empties the slice. See Send() above.
	Flush() error
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

func (sender *sender) Flush() error {
	if len(sender.producerMessages) == 0 {
		return nil
	}

	err := sender.pp.topicProcessor.producer.SendMessages(sender.producerMessages)
	if err != nil {
		sender.pp.logger.Errorf("Message Sender returned error: %s", err)
		return err
	}
	sender.producerMessages = []*sarama.ProducerMessage{}

	return nil
}
