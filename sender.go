package kasper

import (
	"fmt"

	"github.com/Shopify/sarama"
)

// Sender describes an interface for sending messages to Kafka topics
type Sender interface {
	Send(OutgoingMessage) // Send message to output topics
}

type sender struct {
	pp               *partitionProcessor
	incomingMessage  *IncomingMessage
	producerMessages []*sarama.ProducerMessage
}

func newSender(pp *partitionProcessor, incomingMessage *IncomingMessage) *sender {
	return &sender{
		pp,
		incomingMessage,
		[]*sarama.ProducerMessage{},
	}
}

func (sender *sender) createInFlightMessageGroup(committed bool) *inFlightMessageGroup {
	res := inFlightMessageGroup{
		incomingMessage:  sender.incomingMessage,
		inFlightMessages: nil,
		committed:        committed,
	}
	for _, msg := range sender.producerMessages {
		res.inFlightMessages = append(res.inFlightMessages, &inFlightMessage{
			msg: msg,
			ack: false,
		})
	}
	return &res
}

func (sender *sender) Send(msg OutgoingMessage) {
	topicSerde, ok := sender.pp.topicProcessor.config.TopicSerdes[msg.Topic]
	if !ok {
		panic(fmt.Sprintf("Could not find Serde for topic '%s'", msg.Topic))
	}
	producerMessage := &sarama.ProducerMessage{
		Topic:     msg.Topic,
		Key:       sarama.ByteEncoder(topicSerde.KeySerde.Serialize(msg.Key)),
		Value:     sarama.ByteEncoder(topicSerde.ValueSerde.Serialize(msg.Value)),
		Partition: int32(msg.Partition),
		Metadata:  sender.incomingMessage,
	}
	sender.producerMessages = append(sender.producerMessages, producerMessage)
}
