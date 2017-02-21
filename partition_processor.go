package kasper

import (
	"log"

	"github.com/Shopify/sarama"
	"strconv"
)

type partitionProcessor struct {
	topicProcessor     *TopicProcessor
	coordinator        Coordinator
	consumer           sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
	offsetManagers     map[string]sarama.PartitionOffsetManager
	messageProcessor   MessageProcessor
	inputTopics        []string
	partition          int
}

func (pp *partitionProcessor) consumerMessageChannels() []<-chan *sarama.ConsumerMessage {
	chans := make([]<-chan *sarama.ConsumerMessage, len(pp.partitionConsumers))
	for i, consumer := range pp.partitionConsumers {
		chans[i] = consumer.Messages()
	}
	return chans
}

func newPartitionProcessor(tp *TopicProcessor, mp MessageProcessor, partition int) *partitionProcessor {
	consumer, err := sarama.NewConsumerFromClient(tp.client)
	if err != nil {
		log.Fatal(err)
	}
	partitionConsumers := make([]sarama.PartitionConsumer, len(tp.inputTopics))
	partitionOffsetManagers := make(map[string]sarama.PartitionOffsetManager)
	for i, topic := range tp.inputTopics {
		pom, err := tp.offsetManager.ManagePartition(topic, int32(partition))
		if err != nil {
			log.Fatal(err)
		}
		newestOffset, err := tp.client.GetOffset(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			log.Fatal(err)
		}
		nextOffset, _ := pom.NextOffset()
		if nextOffset > newestOffset {
			nextOffset = sarama.OffsetNewest
		}
		c, err := consumer.ConsumePartition(topic, int32(partition), nextOffset)
		if err != nil {
			log.Fatal(err)
		}
		partitionConsumers[i] = c
		partitionOffsetManagers[topic] = pom
	}
	pp := &partitionProcessor{
		tp,
		nil,
		consumer,
		partitionConsumers,
		partitionOffsetManagers,
		mp,
		tp.inputTopics,
		partition,
	}
	pp.coordinator = &partitionProcessorCoordinator{pp}
	return pp
}

func newBatchPartitionProcessor(topicProcessor *TopicProcessor, processor BatchMessageProcessor, partition int) *partitionProcessor {
	panic("not implemented")
}

func (pp *partitionProcessor) process(consumerMessage *sarama.ConsumerMessage) []*sarama.ProducerMessage {
	topicSerde, ok := pp.topicProcessor.config.TopicSerdes[consumerMessage.Topic]
	if !ok {
		log.Fatalf("Could not find Serde for topic '%s'", consumerMessage.Topic)
	}
	incomingMessage := IncomingMessage{
		Topic:     consumerMessage.Topic,
		Partition: int(consumerMessage.Partition),
		Offset:    consumerMessage.Offset,
		Key:       topicSerde.KeySerde.Deserialize(consumerMessage.Key),
		Value:     topicSerde.ValueSerde.Deserialize(consumerMessage.Value),
		Timestamp: consumerMessage.Timestamp,
	}
	sender := newSender(pp)
	pp.messageProcessor.Process(incomingMessage, sender, pp.coordinator)
	pp.offsetManagers[consumerMessage.Topic].MarkOffset(consumerMessage.Offset + 1, "")
	return sender.producerMessages
}

func (pp *partitionProcessor) processBatch(messages []*sarama.ConsumerMessage) []*sarama.ProducerMessage {
	panic("Not implemented")
}

func (pp *partitionProcessor) countMessagesBehindHighWaterMark() {
	partition := strconv.Itoa(pp.partition)
	highWaterMarks := pp.consumer.HighWaterMarks()
	for _, topic := range pp.topicProcessor.inputTopics {
		offsetManager := pp.offsetManagers[topic]
		currentOffset, _ := offsetManager.NextOffset()
		highWaterMark := highWaterMarks[topic][int32(pp.partition)]
		pp.topicProcessor.messagesBehindHighWaterMark.Set(float64(highWaterMark-currentOffset), topic, partition)
	}
}

func (pp *partitionProcessor) onMetricsTick() {
	pp.countMessagesBehindHighWaterMark()
}

func (pp *partitionProcessor) onShutdown() {
	var err error
	for _, pom := range pp.offsetManagers {
		err = pom.Close()
		if err != nil {
			log.Printf("Cannot close offset manager: %s", err)
		}

	}
	for _, pc := range pp.partitionConsumers {
		err = pc.Close()
		if err != nil {
			log.Printf("Cannot close partition consumer: %s", err)
		}
	}
	err = pp.consumer.Close()
	if err != nil {
		log.Fatal(err)
	}
}
