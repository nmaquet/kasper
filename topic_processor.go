/*

kasper is a lightweight Kafka stream processing library.

*/

package kasper

import (
	"log"
	"sync"
	"time"
	"strconv"

	"github.com/Shopify/sarama"
)

// TopicProcessor describes kafka topic processor
type TopicProcessor struct {
	config              *TopicProcessorConfig
	containerID         int
	client              sarama.Client
	producer            sarama.AsyncProducer
	offsetManager       sarama.OffsetManager
	partitionProcessors map[int32]*partitionProcessor
	inputTopics         []string
	partitions          []int
	shutdown            chan struct{}
	waitGroup           sync.WaitGroup
	batchSize           int
	batchWaitDuration   time.Duration

	processCount                Counter
	markOffsetsCount            Counter
	inFlightMessagesCount       Gauge
	messagesBehindHighWaterMark Gauge
}

// MessageProcessor describes kafka message processor
type MessageProcessor interface {
	// Process message from Kafka input topics.
	// This is the function where you perform all needed actions, like
	// population KV storage or producing Kafka output messages
	Process(IncomingMessage, Sender, Coordinator)
}

// TBD
type BatchMessageProcessor interface {
	// TBD
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
		batchSize:           1,
		batchWaitDuration:   0,
	}
	setupMetrics(&topicProcessor, config.Config.MetricsProvider)
	for _, partition := range partitions {
		processor := makeProcessor()
		partitionProcessors[int32(partition)] = newPartitionProcessor(&topicProcessor, processor, partition)
	}
	return &topicProcessor
}

// TBD
type BatchingOpts struct {
	makeProcessor     func() BatchMessageProcessor
	batchSize         int
	batchWaitDuration time.Duration
}

// TBD
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
		batchSize:           opts.batchSize,
		batchWaitDuration:   opts.batchWaitDuration,
	}
	setupMetrics(&topicProcessor, config.Config.MetricsProvider)
	for _, partition := range partitions {
		processor := opts.makeProcessor()
		partitionProcessors[int32(partition)] = newBatchPartitionProcessor(&topicProcessor, processor, partition)
	}
	return &topicProcessor
}

func setupMetrics(tp *TopicProcessor, provider MetricsProvider) {
	tp.processCount = provider.NewCounter("process_count", "Number of times Process() is called", "topic", "partition")
	tp.markOffsetsCount = provider.NewCounter("mark_offset_count", "Number of times MarkOffsets() is called")
	tp.inFlightMessagesCount = provider.NewGauge("in_flight_messages_count", "Number of messages sent but not acked", "topic", "partition")
	tp.messagesBehindHighWaterMark = provider.NewGauge("messages_behind_high_water_mark_count", "Number of messages remaining to consume on the topic/partition", "topic", "partition")
}

func mustHaveValidConfig(config *TopicProcessorConfig, containerID int) {
	if containerID < 0 || containerID >= config.ContainerCount {
		log.Fatalf("ContainerID expected to be between 0 and %d, got: %d", config.ContainerCount-1, containerID)
	}
	for _, topic := range config.InputTopics {
		_, ok := config.TopicSerdes[topic]
		if !ok {
			log.Fatalf("Could not find Serde for topic '%s'", topic)
		}
	}
}

func mustSetupClient(config *TopicProcessorConfig, containerID int) (sarama.Client, []int, sarama.OffsetManager) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest // TODO: make this configurable
	client, err := sarama.NewClient(config.BrokerList, saramaConfig)
	if err != nil {
		log.Fatal(err)
	}
	partitions := config.partitionsForContainer(containerID)
	for _, partition := range partitions {
		_, ok := config.PartitionToContainerID[partition]
		if !ok {
			log.Print("Could not find PartitionToContainerID mapping for partition ", partition)
		}
	}
	offsetManager, err := sarama.NewOffsetManagerFromClient(config.kafkaConsumerGroup(), client)
	if err != nil {
		log.Fatal(err)
	}
	return client, partitions, offsetManager
}

// Start launches a deferred routine for topic processing.
func (tp *TopicProcessor) Start() {
	tp.waitGroup.Add(1)
	go func() {
		defer tp.waitGroup.Done()
		tp.runLoop()
	}()
}

// Shutdown safely shuts down topic processing, waiting for unfinished jobs
func (tp *TopicProcessor) Shutdown() {
	close(tp.shutdown)
	tp.waitGroup.Wait()
}

func (tp *TopicProcessor) runLoop() {
	consumerChan := tp.getConsumerMessagesChan()
	metricsTicker := time.NewTicker(tp.config.Config.MetricsUpdateInterval)
	var markOffsetsTickerChan <-chan time.Time
	var markOffsetsTicker *time.Ticker

	if tp.config.markOffsetsAutomatically() {
		markOffsetsTicker = time.NewTicker(tp.config.AutoMarkOffsetsInterval)
		markOffsetsTickerChan = markOffsetsTicker.C
	} else {
		markOffsetsTickerChan = make(<-chan time.Time)
	}

	tp.waitGroup.Add(1)
	go func() {
		defer tp.waitGroup.Done()
		for err := range tp.producer.Errors() {
			tp.onProducerError(err)
		}
	}()

	for {
		select {
		case consumerMessage := <-consumerChan:
			tp.processConsumerMessage(consumerMessage, markOffsetsTickerChan)
		case msg, more := <-tp.producer.Successes():
			tp.onProducerAck(msg, more)
		case <-markOffsetsTickerChan:
			tp.onMarkOffsetsTick()
		case <-metricsTicker.C:
			tp.onMetricsTick()
		case <-tp.shutdown:
			tp.onShutdown(markOffsetsTicker, metricsTicker)
			return
		}
	}
}

func (tp *TopicProcessor) processConsumerMessage(consumerMessage *sarama.ConsumerMessage, tickerChan <-chan time.Time) {
	tp.processCount.Inc(consumerMessage.Topic, strconv.Itoa(int(consumerMessage.Partition)))
	pp := tp.partitionProcessors[consumerMessage.Partition]
	for {
		if pp.isReadyForMessage(consumerMessage) {
			producerMessages, mustCommit := pp.process(consumerMessage)
			for len(producerMessages) > 0 {
				select {
				case tp.producer.Input() <- producerMessages[0]:
					producerMessages = producerMessages[1:]
				case msg, more := <-tp.producer.Successes():
					tp.onProducerAck(msg, more)
				}
			}
			pp.onProcessCompleted()
			if mustCommit {
				for {
					if pp.isReadyToCommit() {
						tp.config.Config.MarkOffsetsHook()
						pp.commit()
						break
					}
					msg, more := <-tp.producer.Successes()
					tp.onProducerAck(msg, more)
				}
			}
			break
		} else {
			select {
			case msg, more := <-tp.producer.Successes():
				tp.onProducerAck(msg, more)
			}
		}
	}
}

func (tp *TopicProcessor) onShutdown(tickers ...*time.Ticker) {
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
		log.Fatal(err)
	}
	err = tp.client.Close()
	if err != nil {
		log.Fatal(err)
	}
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

func (tp *TopicProcessor) onProducerError(error *sarama.ProducerError) {
	log.Fatal(error) /* FIXME Handle this gracefully with a retry count / backoff period */
}

func (tp *TopicProcessor) onMarkOffsetsTick() {
	tp.markOffsetsCount.Inc()
	tp.config.Config.MarkOffsetsHook()
	for _, pp := range tp.partitionProcessors {
		pp.onMarkOffsetsTick()
	}
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

func mustSetupProducer(brokers []string, producerClientID string, requiredAcks sarama.RequiredAcks) sarama.AsyncProducer {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = producerClientID
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	saramaConfig.Producer.RequiredAcks = requiredAcks

	producer, err := sarama.NewAsyncProducer(brokers, saramaConfig)
	if err != nil {
		log.Fatal(err)
	}

	return producer
}

func (tp *TopicProcessor) onProducerAck(producerMessage *sarama.ProducerMessage, more bool) {
	if !more {
		return
	}
	incomingMessage := producerMessage.Metadata.(*IncomingMessage)
	pp := tp.partitionProcessors[int32(incomingMessage.Partition)]
	pp.onProducerAck(producerMessage)
}
