package kasper

import (
	"strconv"

	"github.com/Shopify/sarama"
)

type partitionProcessor struct {
	topicProcessor     *TopicProcessor
	consumer           sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
	offsetManagers     map[string]sarama.PartitionOffsetManager
	messageProcessor   MessageProcessor
	inputTopics        []string
	partition          int
	logger             Logger
}

func (pp *partitionProcessor) consumerMessageChannels() []<-chan *sarama.ConsumerMessage {
	chans := make([]<-chan *sarama.ConsumerMessage, len(pp.partitionConsumers))
	for i, consumer := range pp.partitionConsumers {
		chans[i] = consumer.Messages()
	}
	return chans
}

func getPartitionOffsetManager(tp *TopicProcessor, topic string, partition int) sarama.PartitionOffsetManager {
	pom, err := tp.offsetManager.ManagePartition(topic, int32(partition))
	if err != nil {
		tp.logger.Panic(err)
	}
	return pom
}

func getPartitionConsumer(tp *TopicProcessor, consumer sarama.Consumer, pom sarama.PartitionOffsetManager, topic string, partition int) sarama.PartitionConsumer {
	newestOffset, err := tp.config.Client.GetOffset(topic, int32(partition), sarama.OffsetNewest)
	if err != nil {
		tp.logger.Panic(err)
	}
	nextOffset, _ := pom.NextOffset()
	if nextOffset > newestOffset {
		nextOffset = sarama.OffsetNewest
	}
	c, err := consumer.ConsumePartition(topic, int32(partition), nextOffset)
	if err != nil {
		tp.logger.Panic(err)
	}
	return c
}

func newPartitionProcessor(tp *TopicProcessor, mp MessageProcessor, partition int) *partitionProcessor {
	consumer, err := sarama.NewConsumerFromClient(tp.config.Client)
	if err != nil {
		tp.logger.Panic(err)
	}
	partitionConsumers := make([]sarama.PartitionConsumer, len(tp.inputTopics))
	partitionOffsetManagers := make(map[string]sarama.PartitionOffsetManager)
	for i, topic := range tp.inputTopics {
		partitionOffsetManager := getPartitionOffsetManager(tp, topic, partition)
		partitionConsumer := getPartitionConsumer(tp, consumer, partitionOffsetManager, topic, partition)
		partitionConsumers[i] = partitionConsumer
		partitionOffsetManagers[topic] = partitionOffsetManager
	}
	pp := &partitionProcessor{
		tp,
		consumer,
		partitionConsumers,
		partitionOffsetManagers,
		mp,
		tp.inputTopics,
		partition,
		tp.logger,
	}
	return pp
}

func (pp *partitionProcessor) process(msgs []*sarama.ConsumerMessage) []*sarama.ProducerMessage {
	sender := newSender(pp)
	pp.messageProcessor.Process(msgs, sender)
	return sender.producerMessages
}

func (pp *partitionProcessor) countMessagesBehindHighWaterMark() {
	partition := strconv.Itoa(pp.partition)
	highWaterMarks := pp.consumer.HighWaterMarks()
	for _, topic := range pp.topicProcessor.inputTopics {
		offsetManager := pp.offsetManagers[topic]
		currentOffset, _ := offsetManager.NextOffset()
		highWaterMark := highWaterMarks[topic][int32(pp.partition)]
		if currentOffset == sarama.OffsetNewest {
			pp.topicProcessor.messagesBehindHighWaterMark.Set(0, topic, partition)
		} else if currentOffset != sarama.OffsetOldest {
			messagesBehindHighWaterMark := highWaterMark - currentOffset
			pp.topicProcessor.messagesBehindHighWaterMark.Set(float64(messagesBehindHighWaterMark), topic, partition)
		}
	}
}

func (pp *partitionProcessor) hasConsumedAllMessages() bool {
	highWaterMarks := pp.consumer.HighWaterMarks()
	for _, topic := range pp.topicProcessor.inputTopics {
		offsetManager := pp.offsetManagers[topic]
		currentOffset, _ := offsetManager.NextOffset()
		highWaterMark := highWaterMarks[topic][int32(pp.partition)]
		if highWaterMark != currentOffset {
			pp.logger.Debugf("Topic %s partition %d has messages remaining to consume (offset = %d, hight water mark = %d)", topic, pp.partition, currentOffset, highWaterMark)
			return false
		}
	}
	pp.logger.Debug("Partitions %d of all input topics have been consumed", pp.partition)
	return true
}

func (pp *partitionProcessor) onMetricsTick() {
	pp.countMessagesBehindHighWaterMark()
}

func (pp *partitionProcessor) markOffsets(messages []*sarama.ConsumerMessage) {
	latestOffset := make(map[string]int64)
	for _, message := range messages {
		latestOffset[message.Topic] = message.Offset
	}
	for topic, offset := range latestOffset {
		pp.logger.Debugf("Marking offset %s:%d", topic, offset+1)
		pp.offsetManagers[topic].MarkOffset(offset+1, "")
	}
}

func (pp *partitionProcessor) onClose() {
	var err error
	for _, pom := range pp.offsetManagers {
		err = pom.Close()
		if err != nil {
			pp.logger.Panicf("Cannot close offset manager: %s", err)
		}

	}
	for _, pc := range pp.partitionConsumers {
		err = pc.Close()
		if err != nil {
			pp.logger.Panicf("Cannot close partition consumer: %s", err)
		}
	}
	err = pp.consumer.Close()
	if err != nil {
		pp.logger.Panic(err)
	}
}
