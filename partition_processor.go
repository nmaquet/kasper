package kasper

import (
	"github.com/Shopify/sarama"
	"log"
)

type PartitionProcessorContext struct {
	// TODO
}

type MessageProcessor interface {
	Process(IncomingMessage, Sender, Coordinator)
}

type Initializer interface {
	Initialize(TopicProcessorConfig, PartitionProcessorContext)
}

type partitionProcessor struct {
	topicProcessor   *TopicProcessor
	coordinator      Coordinator
	sender           Sender
	consumers        []sarama.PartitionConsumer
	messageProcessor MessageProcessor
	inputTopics      []string
	partition        int32
}

func (pp *partitionProcessor) messageChannels() []<-chan *sarama.ConsumerMessage {
	chans := make([]<-chan *sarama.ConsumerMessage, len(pp.consumers))
	for i, consumer := range pp.consumers {
		chans[i] = consumer.Messages()
	}
	return chans
}

func newPartitionProcessor(tp *TopicProcessor, mp MessageProcessor, partition int32, offset int64) *partitionProcessor {
	// FIXME store the consumer? close it?
	consumer, err := sarama.NewConsumerFromClient(tp.client)
	if err != nil {
		log.Fatal(err)
	}
	partitionConsumers := make([]sarama.PartitionConsumer, len(tp.inputTopics))
	for i, topic := range tp.inputTopics {
		c, err := consumer.ConsumePartition(topic, partition, offset)
		if err != nil {
			log.Fatal(err)
		}
		partitionConsumers[i] = c
	}
	var coordinator Coordinator = nil // FIXME
	var sender Sender = nil           // FIXME
	return &partitionProcessor{
		tp,
		coordinator,
		sender,
		partitionConsumers,
		mp,
		tp.inputTopics,
		partition,
	}
}
