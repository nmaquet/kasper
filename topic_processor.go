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
	config              *TopicProcessorConfig
	containerID         int
	client              sarama.Client
	producer            sarama.SyncProducer
	offsetManager       sarama.OffsetManager
	partitionProcessors map[int32]*partitionProcessor
	inputTopics         []string
	partitions          []int
	shutdown            chan struct{}
	waitGroup           sync.WaitGroup
	batchingEnabled     bool
	batchSize           int
	batchWaitDuration   time.Duration

	incomingMessageCount        Counter
	outgoingMessageCount        Counter
	messagesBehindHighWaterMark Gauge
}

// MessageProcessor describes kafka message processor.
// It can be useful if you use external data sources that support bulk requests.
type MessageProcessor interface {
	// Process message from Kafka input topics.
	// This is the function where you perform all needed actions, like
	// population KV storage or producing Kafka output messages
	Process(IncomingMessage, Sender, Coordinator)
}

// BatchMessageProcessor processes several messages at once.
type BatchMessageProcessor interface {
	// ProcessBatch gets an array of incoming Kafka messages.
	// Use Sender to send messages to output topics.
	ProcessBatch([]*IncomingMessage, Sender, Coordinator)
}

// NewTopicProcessor creates a new TopicProcessor with the given config.
// It requires a factory function that creates MessageProcessor instances and a container id.
// The container id must be a number between 0 and config.ContainerCount - 1.
func NewTopicProcessor(config *TopicProcessorConfig, makeProcessor func() MessageProcessor, containerID int) *TopicProcessor {
	mustHaveValidConfig(config, containerID)
	inputTopics := config.InputTopics
	client, partitions, offsetManager := mustSetupClient(config, containerID)
	partitionProcessors := make(map[int32]*partitionProcessor, len(partitions))
	requiredAcks := config.Config.RequiredAcks
	producer := mustSetupProducer(config.BrokerList, config.producerClientID(containerID), requiredAcks)
	topicProcessor := TopicProcessor{
		config:              config,
		containerID:         containerID,
		client:              client,
		producer:            producer,
		offsetManager:       offsetManager,
		partitionProcessors: partitionProcessors,
		inputTopics:         inputTopics,
		partitions:          partitions,
		shutdown:            make(chan struct{}),
		waitGroup:           sync.WaitGroup{},
		batchingEnabled:     false,
	}
	setupMetrics(&topicProcessor, config.Config.MetricsProvider)
	for _, partition := range partitions {
		processor := makeProcessor()
		partitionProcessors[int32(partition)] = newPartitionProcessor(&topicProcessor, processor, nil, partition)
	}
	return &topicProcessor
}

// BatchingOpts set baching options for BatchMessageProcessor
type BatchingOpts struct {
	// Function that return ne winstance of BatchMessageProcessor
	MakeProcessor func() BatchMessageProcessor
	// Max amount of messages to be processed at once in BatchMessageProcessor.Process.
	// Actual amount of messages may be less then BatchSize.
	BatchSize int
	// BatchMessageProcessor.Process is executed for incoming messages if
	// BatchSize has not been reached in BatchWaitDuration.
	// If there are no incoming messages, BatchMessageProcessor.Process is not executed.
	BatchWaitDuration time.Duration
}

// NewBatchTopicProcessor creates a new instance of BatchMessageProcessor
func NewBatchTopicProcessor(config *TopicProcessorConfig, opts BatchingOpts, containerID int) *TopicProcessor {
	mustHaveValidConfig(config, containerID)
	inputTopics := config.InputTopics
	client, partitions, offsetManager := mustSetupClient(config, containerID)
	partitionProcessors := make(map[int32]*partitionProcessor, len(partitions))
	producer := mustSetupProducer(config.BrokerList, config.producerClientID(containerID), config.Config.RequiredAcks)
	topicProcessor := TopicProcessor{
		config:              config,
		containerID:         containerID,
		client:              client,
		producer:            producer,
		offsetManager:       offsetManager,
		partitionProcessors: partitionProcessors,
		inputTopics:         inputTopics,
		partitions:          partitions,
		shutdown:            make(chan struct{}),
		waitGroup:           sync.WaitGroup{},
		batchingEnabled:     true,
		batchSize:           opts.BatchSize,
		batchWaitDuration:   opts.BatchWaitDuration,
	}
	setupMetrics(&topicProcessor, config.Config.MetricsProvider)
	for _, partition := range partitions {
		processor := opts.MakeProcessor()
		partitionProcessors[int32(partition)] = newPartitionProcessor(&topicProcessor, nil, processor, partition)
	}
	return &topicProcessor
}

func setupMetrics(tp *TopicProcessor, provider MetricsProvider) {
	tp.incomingMessageCount = provider.NewCounter("incoming_message_count", "Number of incoming messages received", "topic", "partition")
	tp.outgoingMessageCount = provider.NewCounter("outgoing_message_count", "Number of outgoing messages sent", "topic", "partition")
	tp.messagesBehindHighWaterMark = provider.NewGauge("messages_behind_high_water_mark_count", "Number of messages remaining to consume on the topic/partition", "topic", "partition")
}

func mustHaveValidConfig(config *TopicProcessorConfig, containerID int) {
	if containerID < 0 || containerID >= config.ContainerCount {
		logger.Panicf("ContainerID expected to be between 0 and %d, got: %d", config.ContainerCount-1, containerID)
	}
	for _, topic := range config.InputTopics {
		_, ok := config.TopicSerdes[topic]
		if !ok {
			logger.Panicf("Could not find Serde for topic '%s'", topic)
		}
	}
}

func mustSetupClient(config *TopicProcessorConfig, containerID int) (sarama.Client, []int, sarama.OffsetManager) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest // TODO: make this configurable
	client, err := sarama.NewClient(config.BrokerList, saramaConfig)
	if err != nil {
		logger.Panic(err)
	}
	logger.Info("Connected to Kafka Brokers", config.BrokerList)
	partitions := config.partitionsForContainer(containerID)
	for _, partition := range partitions {
		_, ok := config.PartitionToContainerID[partition]
		if !ok {
			logger.Panic("Could not find PartitionToContainerID mapping for partition ", partition)
		}
	}
	offsetManager, err := sarama.NewOffsetManagerFromClient(config.kafkaConsumerGroup(), client)
	if err != nil {
		logger.Panic(err)
	}
	return client, partitions, offsetManager
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

// Shutdown safely shuts down topic processing, waiting for unfinished jobs
func (tp *TopicProcessor) Shutdown() {
	logger.Info("Received shutdown request")
	close(tp.shutdown)
	tp.waitGroup.Wait()
}

// HasConsumedAllMessages returns true when all input topics have been entirely consumed
func (tp *TopicProcessor) HasConsumedAllMessages() bool {
	logger.Debugf("Checking wheter we have more messages to consume")
	for _, partition := range tp.partitions {
		if ! tp.partitionProcessors[int32(partition)].hasConsumedAllMessages() {
			return false
		}
	}
	logger.Debug("No more messages to consume")
	return true
}

func (tp *TopicProcessor) runLoop() {
	consumerChan := tp.getConsumerMessagesChan()
	metricsTicker := time.NewTicker(tp.config.Config.MetricsUpdateInterval)
	var batchTickerChan <-chan time.Time
	var batchTicker *time.Ticker

	if tp.batchingEnabled {
		batchTicker = time.NewTicker(tp.batchWaitDuration)
		batchTickerChan = batchTicker.C
	} else {
		batchTickerChan = make(<-chan time.Time)
	}

	batches := make(map[int][]*sarama.ConsumerMessage)
	lengths := make(map[int]int)

	for _, partition := range tp.partitions {
		batches[partition] = make([]*sarama.ConsumerMessage, tp.batchSize)
		lengths[partition] = 0
	}

	logger.Info("Entering run loop")

	for {
		select {
		case consumerMessage := <-consumerChan:
			logger.Debugf("Received: %s", consumerMessage)
			if tp.batchingEnabled {
				partition := int(consumerMessage.Partition)
				batches[partition][lengths[partition]] = consumerMessage
				lengths[partition]++
				if lengths[partition] == tp.batchSize {
					logger.Debugf("Processing batch of %d messages...", tp.batchSize)
					tp.processConsumerMessageBatch(batches[partition], partition)
					lengths[partition] = 0
					logger.Debug("Processing of batch complete")
				}
			} else {
				logger.Debug("Processing message...")
				tp.processConsumerMessage(consumerMessage)
				logger.Debug("Processing of message complete")
			}
		case <-metricsTicker.C:
			tp.onMetricsTick()
		case <-batchTickerChan:
			for _, partition := range tp.partitions {
				if lengths[partition] == 0 {
					continue
				}
				logger.Debugf("Processing batch of %d messages...", lengths[partition])
				tp.processConsumerMessageBatch(batches[partition][0:lengths[partition]], partition)
				lengths[partition] = 0
				logger.Debug("Processing of batch complete")
			}
		case <-tp.shutdown:
			tp.onShutdown(metricsTicker, batchTicker)
			return
		}
	}
}

func (tp *TopicProcessor) processConsumerMessage(consumerMessage *sarama.ConsumerMessage) {
	tp.incomingMessageCount.Inc(consumerMessage.Topic, strconv.Itoa(int(consumerMessage.Partition)))
	pp := tp.partitionProcessors[consumerMessage.Partition]
	producerMessages := pp.process(consumerMessage)
	if len(producerMessages) > 0 {
		logger.Debugf("Producing %d Kafka messages...", len(producerMessages))
		err := tp.producer.SendMessages(producerMessages)
		logger.Debug("Producing of Kafka messages complete")
		if err != nil {
			tp.onProducerError(err)
		}
	}
	pp.markOffsets(consumerMessage)
	for _, message := range producerMessages {
		tp.outgoingMessageCount.Inc(message.Topic, strconv.Itoa(int(message.Partition)))
	}
}

func (tp *TopicProcessor) processConsumerMessageBatch(messages []*sarama.ConsumerMessage, partition int) {
	for _, message := range messages {
		tp.incomingMessageCount.Inc(message.Topic, strconv.Itoa(int(message.Partition)))
	}
	pp := tp.partitionProcessors[int32(partition)]
	producerMessages := pp.processBatch(messages)
	if len(producerMessages) > 0 {
		logger.Debugf("Producing %d Kafka messages...", len(producerMessages))
		err := tp.producer.SendMessages(producerMessages)
		logger.Debug("Producing of Kafka messages complete")
		if err != nil {
			tp.onProducerError(err)
		}
	}
	pp.markOffsetsForBatch(messages)
	for _, message := range producerMessages {
		tp.outgoingMessageCount.Inc(message.Topic, strconv.Itoa(int(message.Partition)))
	}
}

func (tp *TopicProcessor) onShutdown(tickers ...*time.Ticker) {
	logger.Info("Shutting down topic processor...")
	for _, ticker := range tickers {
		if ticker != nil {
			ticker.Stop()
		}
	}
	for _, pp := range tp.partitionProcessors {
		pp.onShutdown()
	}
	err := tp.producer.Close()
	if err != nil {
		logger.Panic(err)
	}
	err = tp.client.Close()
	if err != nil {
		logger.Panic(err)
	}
	logger.Info("Shutdown complete")
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
				case <-tp.shutdown:
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

func mustSetupProducer(brokers []string, producerClientID string, requiredAcks sarama.RequiredAcks) sarama.SyncProducer {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = producerClientID
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	saramaConfig.Producer.RequiredAcks = requiredAcks

	producer, err := sarama.NewSyncProducer(brokers, saramaConfig)
	if err != nil {
		logger.Panic(err)
	}

	return producer
}
