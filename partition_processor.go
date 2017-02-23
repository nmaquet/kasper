package kasper

import (
	"log"
	"strconv"

	"github.com/Shopify/sarama"
)

type partitionProcessor struct {
	topicProcessor        *TopicProcessor
	coordinator           Coordinator
	consumer              sarama.Consumer
	partitionConsumers    []sarama.PartitionConsumer
	offsetManagers        map[string]sarama.PartitionOffsetManager
	messageProcessor      MessageProcessor
	batchMessageProcessor BatchMessageProcessor
	inputTopics           []string
	partition             int
}

func (pp *partitionProcessor) consumerMessageChannels() []<-chan *sarama.ConsumerMessage {
	chans := make([]<-chan *sarama.ConsumerMessage, len(pp.partitionConsumers))
	for i, consumer := range pp.partitionConsumers {
		chans[i] = consumer.Messages()
	}
	return chans
}

func newPartitionProcessor(tp *TopicProcessor, mp MessageProcessor, bmp BatchMessageProcessor, partition int) *partitionProcessor {
	if (mp == nil && bmp == nil) || (mp != nil && bmp != nil) {
		panic("Exactly one message processor must be provided")
	}
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
		bmp,
		tp.inputTopics,
		partition,
	}
	pp.coordinator = &partitionProcessorCoordinator{pp}
	return pp
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
	return sender.producerMessages
}

func (pp *partitionProcessor) processBatch(messages []*sarama.ConsumerMessage) []*sarama.ProducerMessage {
	incomingMessages := make([]*IncomingMessage, len(messages))
	for i, message := range messages {
		topicSerde, ok := pp.topicProcessor.config.TopicSerdes[message.Topic]
		if !ok {
			log.Fatalf("Could not find Serde for topic '%s'", message.Topic)
		}
		incomingMessages[i] = &IncomingMessage{
			Topic:     message.Topic,
			Partition: int(message.Partition),
			Offset:    message.Offset,
			Key:       topicSerde.KeySerde.Deserialize(message.Key),
			Value:     topicSerde.ValueSerde.Deserialize(message.Value),
			Timestamp: message.Timestamp,
		}
	}
	sender := newSender(pp)
	pp.batchMessageProcessor.ProcessBatch(incomingMessages, sender, pp.coordinator)
	return sender.producerMessages
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

func (pp *partitionProcessor) markOffsets(message *sarama.ConsumerMessage) {
	pp.offsetManagers[message.Topic].MarkOffset(message.Offset+1, "")
}

func (pp *partitionProcessor) markOffsetsForBatch(messages []*sarama.ConsumerMessage) {
	latestOffset := make(map[string]int64)
	for _, message := range messages {
		latestOffset[message.Topic] = message.Offset
	}
	for topic, offset := range latestOffset {
		pp.offsetManagers[topic].MarkOffset(offset+1, "")
	}
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
