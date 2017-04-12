/*

kasper is a lightweight Kafka stream processing library.

*/

package kasper

import (
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// TopicProcessor describes kafka topic processor
type TopicProcessor struct {
	config              *Config
	producer            sarama.SyncProducer
	offsetManager       sarama.OffsetManager
	partitionProcessors map[int32]*partitionProcessor
	inputTopics         []string
	partitions          []int
	close               chan struct{}
	waitGroup           sync.WaitGroup

	incomingMessageCount        Counter
	outgoingMessageCount        Counter
	messagesBehindHighWaterMark Gauge
}

// MessageProcessor processes several messages at once.
type MessageProcessor interface {
	// Process gets an array of incoming Kafka messages.
	// Use Sender to send messages to output topics.
	Process([]*sarama.ConsumerMessage, Sender, Coordinator)
}

// NewTopicProcessor creates a new instance of MessageProcessor
func NewTopicProcessor(config *Config, makeProcessor func() MessageProcessor) *TopicProcessor {
	config.SetDefaults()
	inputTopics := config.InputTopics
	partitions := config.InputPartitions
	offsetManager := mustSetupOffsetManager(config)
	partitionProcessors := make(map[int32]*partitionProcessor, len(partitions))
	producer := mustSetupProducer(config.Client)
	topicProcessor := TopicProcessor{
		config:              config,
		producer:            producer,
		offsetManager:       offsetManager,
		partitionProcessors: partitionProcessors,
		inputTopics:         inputTopics,
		partitions:          partitions,
		close:               make(chan struct{}),
		waitGroup:           sync.WaitGroup{},
	}
	setupMetrics(&topicProcessor, config.MetricsProvider)
	for _, partition := range partitions {
		processor := makeProcessor()
		partitionProcessors[int32(partition)] = newPartitionProcessor(&topicProcessor, processor, partition)
	}
	return &topicProcessor
}

func setupMetrics(tp *TopicProcessor, provider MetricsProvider) {
	tp.incomingMessageCount = provider.NewCounter("incoming_message_count", "Number of incoming messages received", "topic", "partition")
	tp.outgoingMessageCount = provider.NewCounter("outgoing_message_count", "Number of outgoing messages sent", "topic", "partition")
	tp.messagesBehindHighWaterMark = provider.NewGauge("messages_behind_high_water_mark_count", "Number of messages remaining to consume on the topic/partition", "topic", "partition")
}

func mustSetupOffsetManager(config *Config) sarama.OffsetManager {
	offsetManager, err := sarama.NewOffsetManagerFromClient(config.kafkaConsumerGroup(), config.Client)
	if err != nil {
		logger.Panic(err)
	}
	return offsetManager
}

// Start launches a deferred routine for topic processing.
func (tp *TopicProcessor) Start() {
	logger.Info("Topic processor started")
	tp.waitGroup.Add(1)
	go func() {
		defer tp.waitGroup.Done()
		tp.runLoop()
	}()
}

// Close safely shuts down topic processing, waiting for unfinished jobs
func (tp *TopicProcessor) Close() {
	logger.Info("Received close request")
	close(tp.close)
	tp.waitGroup.Wait()
}

// HasConsumedAllMessages returns true when all input topics have been entirely consumed
func (tp *TopicProcessor) HasConsumedAllMessages() bool {
	logger.Debugf("Checking wheter we have more messages to consume")
	for _, partition := range tp.partitions {
		if !tp.partitionProcessors[int32(partition)].hasConsumedAllMessages() {
			return false
		}
	}
	logger.Debug("No more messages to consume")
	return true
}

func (tp *TopicProcessor) getBatches() map[int][]*sarama.ConsumerMessage {
	batches := make(map[int][]*sarama.ConsumerMessage)

	for _, partition := range tp.partitions {
		batches[partition] = make([]*sarama.ConsumerMessage, tp.config.BatchSize)
	}

	return batches
}

func (tp *TopicProcessor) runLoop() {
	consumerChan := tp.getConsumerMessagesChan()
	metricsTicker := time.NewTicker(tp.config.MetricsUpdateInterval)
	batchTicker := time.NewTicker(tp.config.BatchWaitDuration)

	batches := tp.getBatches()
	lengths := make(map[int]int)

	logger.Info("Entering run loop")

	for {
		select {
		case consumerMessage := <-consumerChan:
			logger.Debugf("Received: %s", consumerMessage)
			partition := int(consumerMessage.Partition)
			batches[partition][lengths[partition]] = consumerMessage
			lengths[partition]++
			if lengths[partition] == tp.config.BatchSize {
				logger.Debugf("Processing batch of %d messages...", tp.config.BatchSize)
				tp.processConsumerMessageBatch(batches[partition], partition)
				lengths[partition] = 0
				logger.Debug("Processing of batch complete")
			}
		case <-metricsTicker.C:
			tp.onMetricsTick()
		case <-batchTicker.C:
			for _, partition := range tp.partitions {
				if lengths[partition] == 0 {
					continue
				}
				logger.Debugf("Processing batch of %d messages...", lengths[partition])
				tp.processConsumerMessageBatch(batches[partition][0:lengths[partition]], partition)
				lengths[partition] = 0
				logger.Debug("Processing of batch complete")
			}
		case <-tp.close:
			tp.onClose(metricsTicker, batchTicker)
			return
		}
	}
}

func (tp *TopicProcessor) processConsumerMessageBatch(messages []*sarama.ConsumerMessage, partition int) {
	for _, message := range messages {
		tp.incomingMessageCount.Inc(message.Topic, strconv.Itoa(int(message.Partition)))
	}
	pp := tp.partitionProcessors[int32(partition)]
	producerMessages := pp.process(messages)
	if len(producerMessages) > 0 {
		logger.Debugf("Producing %d Kafka messages...", len(producerMessages))
		err := tp.producer.SendMessages(producerMessages)
		logger.Debug("Producing of Kafka messages complete")
		if err != nil {
			tp.onProducerError(err)
		}
	}
	pp.markOffsets(messages)
	for _, message := range producerMessages {
		tp.outgoingMessageCount.Inc(message.Topic, strconv.Itoa(int(message.Partition)))
	}
}

func (tp *TopicProcessor) onClose(tickers ...*time.Ticker) {
	logger.Info("Closing topic processor...")
	for _, ticker := range tickers {
		if ticker != nil {
			ticker.Stop()
		}
	}
	for _, pp := range tp.partitionProcessors {
		pp.onClose()
	}
	err := tp.producer.Close()
	if err != nil {
		logger.Panic(err)
	}
	logger.Info("Close complete")
}

func (tp *TopicProcessor) getConsumerMessagesChan() <-chan *sarama.ConsumerMessage {
	consumerMessagesChan := make(chan *sarama.ConsumerMessage)
	for _, ch := range tp.consumerMessageChannels() {
		tp.waitGroup.Add(1)
		go func(c <-chan *sarama.ConsumerMessage) {
			defer tp.waitGroup.Done()
			for msg := range c {
				select {
				case consumerMessagesChan <- msg:
					continue
				case <-tp.close:
					return
				}

			}
		}(ch)
	}
	return consumerMessagesChan
}

func (tp *TopicProcessor) onProducerError(err error) {
	logger.Panic(err)
}

func (tp *TopicProcessor) onMetricsTick() {
	for _, pp := range tp.partitionProcessors {
		pp.onMetricsTick()
	}
}

func (tp *TopicProcessor) consumerMessageChannels() []<-chan *sarama.ConsumerMessage {
	var chans []<-chan *sarama.ConsumerMessage
	for _, partitionProcessor := range tp.partitionProcessors {
		partitionChannels := partitionProcessor.consumerMessageChannels()
		chans = append(chans, partitionChannels...)
	}
	return chans
}

func mustSetupProducer(client sarama.Client) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		logger.Panic(err)
	}
	return producer
}
