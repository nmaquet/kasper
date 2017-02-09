package kasper

import (
	"log"
	"github.com/Shopify/sarama"
	"time"
)

type TopicProcessor struct {
	config              *TopicProcessorConfig
	containerId         int
	client              sarama.Client
	offsetManager       sarama.OffsetManager
	partitionProcessors []*partitionProcessor
	inputTopics         []string
	partitions          []int32
}

func partitionsOfTopics(topics []string, client sarama.Client) []int32 {
	partitionsSet := make(map[int32]struct{})
	for _, topic := range topics {
		partitions, err := client.Partitions(topic)
		if err != nil {
			log.Fatal(err)
		}
		for _, partition := range partitions {
			partitionsSet[partition] = struct{}{}
		}
	}
	i := 0
	partitions := make([]int32, len(partitionsSet))
	for partition := range partitionsSet {
		partitions[i] = partition
		i++
	}
	return partitions
}

func NewTopicProcessor(config *TopicProcessorConfig, makeProcessor func() MessageProcessor, containerId int) *TopicProcessor {
	// TODO: check all input topics are covered by a Serde
	// TODO: check all input partitions and make sure PartitionAssignment is valid
	// TODO: check containerId is within [0, ContainerCount)
	inputTopics := config.InputTopics
	brokerList := config.BrokerList
	client, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		log.Fatal(err)
	}
	partitions := config.partitionsForContainer(containerId)
	offsetManager, err := sarama.NewOffsetManagerFromClient(config.kafkaConsumerGroup(), client)
	if err != nil {
		log.Fatal(err)
	}
	partitionProcessors := make([]*partitionProcessor, len(partitions))
	topicProcessor := TopicProcessor{
		config,
		containerId,
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
	/* FIXME factor these out to sub-functions */
	consumerMessagesChan := make(chan *sarama.ConsumerMessage)
	for _, ch := range tp.consumerMessageChannels() {
		go func(c <-chan *sarama.ConsumerMessage) {
			for msg := range c {
				consumerMessagesChan <- msg
			}
		}(ch)
	}
	producerSuccessesChan := make(chan *sarama.ProducerMessage)
	for _, ch := range tp.producerSuccessesChannels() {
		go func(c <-chan *sarama.ProducerMessage) {
			for msg := range c {
				producerSuccessesChan <- msg
			}
		}(ch)
	}
	producerErrorsChan := make(chan *sarama.ProducerError)
	for _, ch := range tp.producerErrorsChannels() {
		go func(c <-chan *sarama.ProducerError) {
			for msg := range c {
				producerErrorsChan <- msg
			}
		}(ch)
	}

	/* TODO: call Stop() on this ticker when implementing proper shutdown */
	markOffsetTicker := time.NewTicker(5 * time.Second) /* TODO: make this delay configurable */
	for {
		select {
		case consumerMessage := <-consumerMessagesChan:
			pp := tp.partitionProcessors[consumerMessage.Partition]
			pp.processConsumerMessage(consumerMessage)
		case producerMessage := <-producerSuccessesChan:
			pp := tp.partitionProcessors[producerMessage.Partition]
			pp.processProducerMessageSuccess(producerMessage)
		case producerError := <-producerErrorsChan:
			log.Fatal(producerError) /* FIXME Handle this gracefully with a retry count / backoff period */
		case <-markOffsetTicker.C:
			for _, pp := range tp.partitionProcessors {
				pp.markOffsets()
			}
		}
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
