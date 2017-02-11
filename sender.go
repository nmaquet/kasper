package kasper

import (
	"github.com/Shopify/sarama"
	"log"
)

type Sender interface {
	Send(OutgoingMessage)
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
		log.Fatalf("Could not find Serde for topic '%s'", msg.Topic)
	}
	producerMessage := &sarama.ProducerMessage{
		Topic:     string(msg.Topic),
		Key:       sarama.ByteEncoder(topicSerde.KeySerde.Serialize(msg.Key)),
		Value:     sarama.ByteEncoder(topicSerde.ValueSerde.Serialize(msg.Value)),
		Partition: int32(msg.Partition),
		Metadata:  sender.incomingMessage,
	}
	sender.producerMessages = append(sender.producerMessages, producerMessage)
}