/*

Kasper is a lightweight library for processing Kafka topics.
It is heavily inspired by Apache Samza (See http://samza.apache.org).
Kasper processes Kafka messages in small batches and is designed to work with centralized key-value stores such as Redis,
Cassandra or Elasticsearch for maintaining state during processing. Kasper is a good fit for high-throughput applications
(> 10k messages per second) that can tolerate a moderate amount of processing latency (~1000ms).
Please note that Kasper is designed for idempotent processing of at-least-once semantics streams.
If you require exactly-once semantics or need to perform non-idempotent operations, Kasper is likely not a good choice.

Step 1 - Create a sarama Client

Kasper uses Shopify's excellent sarama library (see https://github.com/Shopify/sarama)
for consuming and producing messages to Kafka. All Kasper application must begin with instantiating a sarama Client.
Choose the parameters in sarama.Config carefully; the performance, reliability, and correctness
of your application are all highly sensitive to these settings.
We recommend setting sarama.Config.Producer.RequiredAcks to WaitForAll and sarama.Config.Producer.Retry.Max to 0.

	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 0
	client, err := sarama.NewClient([]string{"localhost:9092"}, )

Step 2 - create a Config

TopicProcessorName is used for logging, labeling metrics, and is used as a suffix to the Kafka consumer group.
InputTopics and InputPartitions are the lists of topics and partitions to consume.
Please note that Kasper currently does not support consuming topics with differing numbers of partitions.
This limitation can be alleviated by manually adding an extra fan-out step in your processing pipeline
to a new topic with the desired number of partitions.

	config := &kasper.Config{
		TopicProcessorName:    "twitter-reach",
		Client:                client,
		InputTopics:           []string{"tweets", "twitter-followers"},
		InputPartitions:       []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
		BatchSize: 	       10000
		BatchWaitDuration:     5 * time.Second
		Logger: 	       kasper.NewJSONLogger("twitter-reach-0", false)
		MetricsProvider:       kasper.NewPrometheus("twitter-reach-0")
		MetricsUpdateInterval: 60 * time.Second
	}

Kasper is instrumented with a number of useful metrics so we
recommend setting MetricsProvider for production applications. Kasper includes an implementation for collecting
metrics in Prometheus and adapting the interface to other tools should be easy.

Step 3 - Create a MessageProcessor per input partition

You need to create a map[int]MessageProcessor. The MessageProcessor instances can safely be shared across partitions.
Each MessageProcessor must implement a single function:

	func (*TweetProcessor) Process(messages []*sarama.ConsumerMessage, sender Sender) error {
		// process messages here
	}

All messages for the input topics on the specified partition will be passed to the appropriate MessageProcessor instance.
This is useful for implementing partition-wise joins of different topics.
The Sender instance must be used to produce messages to output topics. Messages passed to Sender are not sent directly but are collected in an array instead.
When Process returns, the messages are sent to Kafka and Kasper waits for the configured number of acks.
When all messages have been successfully produced, Kasper updates the consumer offsets of the input partitions and
resumes processing. If Process returns a non-nil error value, Kasper stops all processing.

Step 4 - Create a TopicProcessor

To start processing messages, call TopicProcessor.RunLoop(). Kasper does not spawn any goroutines and runs a single-threaded
event loop instead. RunLoop() will block the current goroutine and will run forever until an error occurs or until
Close() is called.
For parallel processing, run multiple TopicProcessor instances in different goroutines or processes
(the input partitions cannot overlap). You should set Config.TopicProcessorName to the same value on
all instances in order to easily scale the processing up or down.

*/
package kasper

import (
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// TopicProcessor is the main entity in Kasper. It implements a single-threaded processing loop for a set of topics
// and partitions.
type TopicProcessor struct {
	config              *Config
	producer            sarama.SyncProducer
	offsetManager       sarama.OffsetManager
	partitionProcessors map[int32]*partitionProcessor
	inputTopics         []string
	partitions          []int
	close               chan struct{}
	waitGroup           sync.WaitGroup

	logger                      Logger
	incomingMessageCount        Counter
	outgoingMessageCount        Counter
	messagesBehindHighWaterMark Gauge
}

// MessageProcessor is the interface that encapsulates application business logic.
// It receives all messages of a single partition of the TopicProcessor's input topics.
type MessageProcessor interface {
	// Process receives a slice of incoming Kafka messages and a Sender to send messages to output topics.
	// References to the byte slice or Sender interface cannot be held between calls.
	// If Process returns a non-nil error value, Kasper stops all processing.
	// This error value is then returned by TopicProcessor.RunLoop().
	Process([]*sarama.ConsumerMessage, Sender) error
}

// NewTopicProcessor creates a new instance of TopicProcessor.
// For parallel processing, run multiple TopicProcessor instances in different goroutines or processes
// (the input partitions cannot overlap). You should set Config.TopicProcessorName to the same value on
// all instances in order to easily scale the processing up or down.
func NewTopicProcessor(config *Config, messageProcessors map[int]MessageProcessor) *TopicProcessor {
	config.setDefaults()
	inputTopics := config.InputTopics
	partitions := config.InputPartitions
	offsetManager := mustSetupOffsetManager(config)
	partitionProcessors := make(map[int32]*partitionProcessor, len(partitions))
	producer := mustSetupProducer(config)
	provider := config.MetricsProvider
	topicProcessor := TopicProcessor{
		config,
		producer,
		offsetManager,
		partitionProcessors,
		inputTopics,
		partitions,
		make(chan struct{}),
		sync.WaitGroup{},
		config.Logger,
		provider.NewCounter("incoming_message_count", "Number of incoming messages received", "topic", "partition"),
		provider.NewCounter("outgoing_message_count", "Number of outgoing messages sent", "topic", "partition"),
		provider.NewGauge("messages_behind_high_water_mark_count", "Number of messages remaining to consume on the topic/partition", "topic", "partition"),
	}
	for _, partition := range partitions {
		mp, found := messageProcessors[partition]
		if !found {
			config.Logger.Panicf("messageProcessor doesn't contain an entry for partition %d", partition)
		}
		partitionProcessors[int32(partition)] = newPartitionProcessor(&topicProcessor, mp, partition)
	}
	return &topicProcessor
}

func mustSetupOffsetManager(config *Config) sarama.OffsetManager {
	offsetManager, err := sarama.NewOffsetManagerFromClient(config.kafkaConsumerGroup(), config.Client)
	if err != nil {
		config.Logger.Panic(err)
	}
	return offsetManager
}

// Close safely shuts down the TopicProcessor, which makes RunLoop() return.
func (tp *TopicProcessor) Close() {
	tp.logger.Info("Received close request")
	close(tp.close)
	tp.waitGroup.Wait()
}

// HasConsumedAllMessages returns true when all input topics have been entirely consumed.
// Kasper checks all high water marks and offsets for all topics before returning.
func (tp *TopicProcessor) HasConsumedAllMessages() bool {
	tp.logger.Debugf("Checking wheter we have more messages to consume")
	for _, partition := range tp.partitions {
		if !tp.partitionProcessors[int32(partition)].hasConsumedAllMessages() {
			return false
		}
	}
	tp.logger.Debug("No more messages to consume")
	return true
}

func (tp *TopicProcessor) getBatches() map[int][]*sarama.ConsumerMessage {
	batches := make(map[int][]*sarama.ConsumerMessage)

	for _, partition := range tp.partitions {
		batches[partition] = make([]*sarama.ConsumerMessage, tp.config.BatchSize)
	}

	return batches
}

// RunLoop is the main processing loop of Kasper. It does not spawn any goroutines and runs a single-threaded
// event loop instead. RunLoop will block the current goroutine and will run forever until an error occurs or until
// Close() is called. RunLoop propagates the error returned by MessageProcessor.Process if not nil.
func (tp *TopicProcessor) RunLoop() error {
	consumerChan := tp.getConsumerMessagesChan()
	metricsTicker := time.NewTicker(tp.config.MetricsUpdateInterval)
	batchTicker := time.NewTicker(tp.config.BatchWaitDuration)

	batches := tp.getBatches()
	lengths := make(map[int]int)

	tp.logger.Info("Entering run loop")

	for {
		select {
		case consumerMessage := <-consumerChan:
			tp.logger.Debugf("Received: %s", consumerMessage)
			partition := int(consumerMessage.Partition)
			batches[partition][lengths[partition]] = consumerMessage
			lengths[partition]++
			if lengths[partition] == tp.config.BatchSize {
				tp.logger.Debugf("Processing batch of %d messages...", tp.config.BatchSize)
				err := tp.processConsumerMessages(batches[partition], partition)
				if err != nil {
					tp.onClose(metricsTicker, batchTicker)
					return err
				}
				lengths[partition] = 0
				tp.logger.Debug("Processing of batch complete")
			}
		case <-metricsTicker.C:
			tp.onMetricsTick()
		case <-batchTicker.C:
			for _, partition := range tp.partitions {
				if lengths[partition] == 0 {
					continue
				}
				tp.logger.Debugf("Processing batch of %d messages...", lengths[partition])
				err := tp.processConsumerMessages(batches[partition][0:lengths[partition]], partition)
				if err != nil {
					tp.onClose(metricsTicker, batchTicker)
					return err
				}
				lengths[partition] = 0
				tp.logger.Debug("Processing of batch complete")
			}
		case <-tp.close:
			tp.onClose(metricsTicker, batchTicker)
			return nil
		}
	}
}

func (tp *TopicProcessor) processConsumerMessages(messages []*sarama.ConsumerMessage, partition int) error {
	for _, message := range messages {
		tp.incomingMessageCount.Inc(message.Topic, strconv.Itoa(int(message.Partition)))
	}
	pp := tp.partitionProcessors[int32(partition)]
	producerMessages, err := pp.process(messages)
	if err != nil {
		return err
	}
	if len(producerMessages) > 0 {
		tp.logger.Debugf("Producing %d Kafka messages...", len(producerMessages))
		err := tp.producer.SendMessages(producerMessages)
		tp.logger.Debug("Producing of Kafka messages complete")
		if err != nil {
			tp.logger.Errorf("Failed to produce messages: %s", err)
			return err
		}
	}
	pp.markOffsets(messages)
	for _, message := range producerMessages {
		tp.outgoingMessageCount.Inc(message.Topic, strconv.Itoa(int(message.Partition)))
	}
	return nil
}

func (tp *TopicProcessor) onClose(tickers ...*time.Ticker) {
	tp.logger.Info("Closing topic processor...")
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
		tp.logger.Panic(err)
	}
	tp.logger.Info("Close complete")
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

func mustSetupProducer(config *Config) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducerFromClient(config.Client)
	if err != nil {
		config.Logger.Panic(err)
	}
	return producer
}
