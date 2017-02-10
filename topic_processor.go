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
	config              *TopicProcessorConfig
	containerId         ContainerId
	client              sarama.Client
	offsetManager       sarama.OffsetManager
	partitionProcessors []*partitionProcessor
	inputTopics         []Topic
	partitions          []Partition
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
	topicProcessor := TopicProcessor{
		config,
		cid,
		client,
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
	producerSuccessesChan := tp.getProducerMessagesChan()
	producerErrorsChan := tp.getProducerErrorsChan()
	var markOffsetTickerChan  <-chan time.Time
	var markOffsetsTicker *time.Ticker
	if tp.config.AutoMarkOffsetsInterval > 0 {
		/* TODO: call Stop() on this ticker when implementing proper shutdown */
		markOffsetsTicker = time.NewTicker(tp.config.AutoMarkOffsetsInterval)
		markOffsetTickerChan = markOffsetsTicker.C
	} else {
		markOffsetTickerChan = make(<-chan time.Time)
	}
	for {
		select {
		case consumerMessage := <-consumerMessagesChan:
			pp := tp.partitionProcessors[consumerMessage.Partition]
			for {
				if pp.isReadyForMessage(consumerMessage) {
					pp.processConsumerMessage(consumerMessage)
					break
				} else {
					select {
					case msg := <-producerSuccessesChan:
						tp.processProducerMessageSuccess(msg)
					case err := <-producerErrorsChan:
						tp.processProducerError(err)
					case <-markOffsetTickerChan:
						tp.processMarkOffsetsTick()
					}
				}
			}
		case msg := <-producerSuccessesChan:
			tp.processProducerMessageSuccess(msg)
		case err := <-producerErrorsChan:
			tp.processProducerError(err)
		case <-markOffsetTickerChan:
			tp.processMarkOffsetsTick()
		}
	}
}

func (tp *TopicProcessor) getProducerErrorsChan() chan *sarama.ProducerError {
	producerErrorsChan := make(chan *sarama.ProducerError)
	for _, ch := range tp.producerErrorsChannels() {
		go func(c <-chan *sarama.ProducerError) {
			for msg := range c {
				producerErrorsChan <- msg
			}
		}(ch)
	}
	return producerErrorsChan
}

func (tp *TopicProcessor) getProducerMessagesChan() chan *sarama.ProducerMessage {
	producerSuccessesChan := make(chan *sarama.ProducerMessage)
	for _, ch := range tp.producerSuccessesChannels() {
		go func(c <-chan *sarama.ProducerMessage) {
			for msg := range c {
				producerSuccessesChan <- msg
			}
		}(ch)
	}
	return producerSuccessesChan
}

func (tp *TopicProcessor) getConsumerMessagesChan() chan *sarama.ConsumerMessage {
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

func (tp *TopicProcessor) producerSuccessesChannels() []<-chan *sarama.ProducerMessage {
	var chans []<-chan *sarama.ProducerMessage
	for _, partitionProcessor := range tp.partitionProcessors {
		ch := partitionProcessor.producer.Successes()
		chans = append(chans, ch)
	}
	return chans
}

func (tp *TopicProcessor) producerErrorsChannels() []<-chan *sarama.ProducerError {
	var chans []<-chan *sarama.ProducerError
	for _, partitionProcessor := range tp.partitionProcessors {
		ch := partitionProcessor.producer.Errors()
		chans = append(chans, ch)
	}
	return chans
}
