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
	producerMessages []*sarama.ProducerMessage
}

func newSender(pp *partitionProcessor) *sender {
	return &sender{
		pp,
		[]*sarama.ProducerMessage{},
	}
}

func (sender *sender) Send(msg OutgoingMessage) {
	topicSerde, ok := sender.pp.topicProcessor.config.TopicSerdes[msg.Topic]
	if !ok {
		logger.Panic(fmt.Sprintf("Could not find Serde for topic '%s'", msg.Topic))
	}
	producerMessage := &sarama.ProducerMessage{
		Topic:     msg.Topic,
		Key:       sarama.ByteEncoder(topicSerde.KeySerde.Serialize(msg.Key)),
		Value:     sarama.ByteEncoder(topicSerde.ValueSerde.Serialize(msg.Value)),
		Partition: int32(msg.Partition),
	}
	sender.producerMessages = append(sender.producerMessages, producerMessage)
}
