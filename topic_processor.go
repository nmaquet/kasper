/*
kasper is a lightweight Kafka stream processing library.
 */
package kasper

import (
	"log"
	"github.com/Shopify/sarama"
	"time"
)

type TopicProcessor struct {
	config               *TopicProcessorConfig
	containerId          ContainerId
	client               sarama.Client
	producer             sarama.AsyncProducer
	offsetManager        sarama.OffsetManager
	partitionProcessors  []*partitionProcessor
	inputTopics          []Topic
	partitions           []Partition
}

// NewTopicProcessor creates a new TopicProcessor with the given config.
// It requires a factory function that creates MessageProcessor instances and a container id.
// The container id must be a number between 0 and config.ContainerCount - 1.
func NewTopicProcessor(config *TopicProcessorConfig, makeProcessor func() MessageProcessor, cid ContainerId) *TopicProcessor {
	// TODO: check all input topics are covered by a Serde
	// TODO: check all input partitions and make sure PartitionAssignment is valid
	// TODO: check cid is within [0, ContainerCount)
	inputTopics := config.InputTopics
	brokerList := config.BrokerList
	client, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		log.Fatal(err)
	}
	partitions := config.partitionsForContainer(cid)
	offsetManager, err := sarama.NewOffsetManagerFromClient(config.kafkaConsumerGroup(), client)
	if err != nil {
		log.Fatal(err)
	}
	partitionProcessors := make([]*partitionProcessor, len(partitions))
	requiredAcks := config.KasperConfig.RequiredAcks
	producer := mustSetupProducer(config.BrokerList, config.producerClientId(cid), requiredAcks)
	topicProcessor := TopicProcessor{
		config,
		cid,
		client,
		producer,
		offsetManager,
		partitionProcessors,
		inputTopics,
		partitions,
	}
	for i, partition := range partitions {
		processor := makeProcessor()
		partitionProcessors[i] = newPartitionProcessor(&topicProcessor, processor, partition)
	}
	return &topicProcessor
}

func (tp *TopicProcessor) Run() {
	consumerMessagesChan := tp.getConsumerMessagesChan()
	var markOffsetTickerChan <-chan time.Time
	var markOffsetsTicker *time.Ticker
	if tp.config.AutoMarkOffsetsInterval > 0 {
		/* TODO: call Stop() on this ticker when implementing proper shutdown */
		markOffsetsTicker = time.NewTicker(tp.config.AutoMarkOffsetsInterval)
		markOffsetTickerChan = markOffsetsTicker.C
	} else {
		markOffsetTickerChan = make(<-chan time.Time)
	}
	go func() {
		err := <-tp.producer.Errors()
		tp.processProducerError(err)
	}()
	for {
		select {
		case consumerMessage := <-consumerMessagesChan:
			pp := tp.partitionProcessors[consumerMessage.Partition]
			for {
				if pp.isReadyForMessage(consumerMessage) {
					producerMessages := pp.processConsumerMessage(consumerMessage)
					for len(producerMessages) > 0 {
						select {
						case tp.producer.Input() <- producerMessages[0]:
							producerMessages = producerMessages[1:]
						case msg := <-tp.producer.Successes():
							tp.processProducerMessageSuccess(msg)
						case <-markOffsetTickerChan:
							tp.processMarkOffsetsTick()
						}
					}
					pp.pruneInFlightMessageGroups()
					break
				} else {
					select {
					case msg := <-tp.producer.Successes():
						tp.processProducerMessageSuccess(msg)
					case <-markOffsetTickerChan:
						tp.processMarkOffsetsTick()
					}
				}
			}
		case msg := <-tp.producer.Successes():
			tp.processProducerMessageSuccess(msg)
		case <-markOffsetTickerChan:
			tp.processMarkOffsetsTick()
		}
	}
}

func (tp *TopicProcessor) getConsumerMessagesChan() <-chan *sarama.ConsumerMessage {
	consumerMessagesChan := make(chan *sarama.ConsumerMessage)
	for _, ch := range tp.consumerMessageChannels() {
		go func(c <-chan *sarama.ConsumerMessage) {
			for msg := range c {
				consumerMessagesChan <- msg
			}
		}(ch)
	}
	return consumerMessagesChan
}

func (tp *TopicProcessor) processProducerError(error *sarama.ProducerError) {
	log.Fatal(error) /* FIXME Handle this gracefully with a retry count / backoff period */
}

func (tp *TopicProcessor) processMarkOffsetsTick() {
	for _, pp := range tp.partitionProcessors {
		pp.markOffsets()
	}
}

func (tp *TopicProcessor) processProducerMessageSuccess(producerMessage *sarama.ProducerMessage) {
	pp := tp.partitionProcessors[producerMessage.Partition]
	pp.processProducerMessageSuccess(producerMessage)
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

func mustSetupProducer(brokers []string, producerClientId string, requiredAcks sarama.RequiredAcks) sarama.AsyncProducer {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = producerClientId
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	saramaConfig.Producer.RequiredAcks = requiredAcks

	producer, err := sarama.NewAsyncProducer(brokers, saramaConfig)
	if err != nil {
		log.Fatal(err)
	}

	return producer
}
