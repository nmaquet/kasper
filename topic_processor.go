/*

kasper is a lightweight Kafka stream processing library.

*/

package kasper

import (
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

type TopicProcessor struct {
	config              *TopicProcessorConfig
	containerID         int
	client              sarama.Client
	producer            sarama.AsyncProducer
	offsetManager       sarama.OffsetManager
	partitionProcessors []*partitionProcessor
	inputTopics         []Topic
	partitions          []int
	shutdown            chan bool
	waitGroup           sync.WaitGroup
}

type MessageProcessor interface {
	Process(IncomingMessage, Sender, Coordinator)
}

// NewTopicProcessor creates a new TopicProcessor with the given config.
// It requires a factory function that creates MessageProcessor instances and a container id.
// The container id must be a number between 0 and config.ContainerCount - 1.
func NewTopicProcessor(config *TopicProcessorConfig, makeProcessor func() MessageProcessor, containerID int) *TopicProcessor {
	// TODO: check all input topics are covered by a Serde
	// TODO: check all input partitions and make sure PartitionAssignment is valid
	// TODO: check cid is within [0, ContainerCount)
	inputTopics := config.InputTopics
	brokerList := config.BrokerList
	client, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		log.Fatal(err)
	}
	partitions := config.partitionsForContainer(containerID)
	offsetManager, err := sarama.NewOffsetManagerFromClient(config.kafkaConsumerGroup(), client)
	if err != nil {
		log.Fatal(err)
	}
	partitionProcessors := make([]*partitionProcessor, len(partitions))
	requiredAcks := config.Config.RequiredAcks
	producer := mustSetupProducer(config.BrokerList, config.producerClientID(containerID), requiredAcks)
	topicProcessor := TopicProcessor{
		config,
		containerID,
		client,
		producer,
		offsetManager,
		partitionProcessors,
		inputTopics,
		partitions,
		make(chan bool),
		sync.WaitGroup{},
	}
	for i, partition := range partitions {
		processor := makeProcessor()
		partitionProcessors[i] = newPartitionProcessor(&topicProcessor, processor, partition)
	}
	return &topicProcessor
}

func (tp *TopicProcessor) Start() {
	tp.waitGroup.Add(1)
	go func() {
		defer tp.waitGroup.Done()
		tp.runLoop()
	}()
}

func (tp *TopicProcessor) runLoop() {
	consumerChan := tp.getConsumerMessagesChan()
	var tickerChan <-chan time.Time
	var ticker *time.Ticker

	if tp.config.markOffsetsAutomatically() {
		ticker = time.NewTicker(tp.config.AutoMarkOffsetsInterval)
		tickerChan = ticker.C
	} else {
		tickerChan = make(<-chan time.Time)
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
			pp := tp.partitionProcessors[consumerMessage.Partition]
			for {
				if pp.isReadyForMessage(consumerMessage) {
					producerMessages := pp.process(consumerMessage)
					for len(producerMessages) > 0 {
						select {
						case tp.producer.Input() <- producerMessages[0]:
							producerMessages = producerMessages[1:]
						case msg, more := <-tp.producer.Successes():
							tp.onProducerAck(msg, more)
						case <-tickerChan:
							tp.onTick()
						}
					}
					pp.onProcessCompleted()
					break
				} else {
					select {
					case msg, more := <-tp.producer.Successes():
						tp.onProducerAck(msg, more)
					case <-tickerChan:
						tp.onTick()
					}
				}
			}
		case msg, more := <-tp.producer.Successes():
			tp.onProducerAck(msg, more)
		case <-tickerChan:
			tp.onTick()
		case <-tp.shutdown:
			tp.onShutdown()
			return
		}
	}
}

func (tp *TopicProcessor) onShutdown() {
	for _, pp := range tp.partitionProcessors {
		pp.onShutdown()
	}
	tp.producer.Close()
	tp.client.Close()
}

func (tp *TopicProcessor) getConsumerMessagesChan() <-chan *sarama.ConsumerMessage {
	consumerMessagesChan := make(chan *sarama.ConsumerMessage)
	for _, ch := range tp.consumerMessageChannels() {
		tp.waitGroup.Add(1)
		go func(c <-chan *sarama.ConsumerMessage) {
			defer tp.waitGroup.Done()
			for msg := range c {
				consumerMessagesChan <- msg
			}
		}(ch)
	}
	return consumerMessagesChan
}

func (tp *TopicProcessor) onProducerError(error *sarama.ProducerError) {
	log.Fatal(error) /* FIXME Handle this gracefully with a retry count / backoff period */
}

func (tp *TopicProcessor) onTick() {
	for _, pp := range tp.partitionProcessors {
		pp.markOffsetsIfPossible()
	}
}

func (tp *TopicProcessor) consumerMessageChannels() []<-chan *sarama.ConsumerMessage {
	var chans []<-chan *sarama.ConsumerMessage
	for _, partitionProcessor := range tp.partitionProcessors {
		partitionChannels := partitionProcessor.consumerMessageChannels()
		for _, ch := range partitionChannels {
			chans = append(chans, ch)
		}
	}
	return chans
}

func mustSetupProducer(brokers []string, producerClientID string, requiredAcks sarama.RequiredAcks) sarama.AsyncProducer {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = producerClientID
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	saramaConfig.Producer.RequiredAcks = requiredAcks
	saramaConfig.MetricRegistry = metrics.DefaultRegistry

	producer, err := sarama.NewAsyncProducer(brokers, saramaConfig)
	if err != nil {
		log.Fatal(err)
	}

	return producer
}

func (tp *TopicProcessor) Shutdown() {
	tp.shutdown <- true
	tp.waitGroup.Wait()
}

func (tp *TopicProcessor) onProducerAck(producerMessage *sarama.ProducerMessage, more bool) {
	if !more {
		return
	}
	pp := tp.partitionProcessors[producerMessage.Partition]
	pp.onProducerAck(producerMessage)
}
