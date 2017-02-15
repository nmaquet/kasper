package kasper

import (
	"log"

	"github.com/Shopify/sarama"
)

type partitionProcessor struct {
	topicProcessor                  *TopicProcessor
	coordinator                     Coordinator
	consumer                        sarama.Consumer
	partitionConsumers              []sarama.PartitionConsumer
	offsetManagers                  map[string]sarama.PartitionOffsetManager
	messageProcessor                MessageProcessor
	inputTopics                     []string
	partition                       int
	inFlightMessageGroups           map[string][]*inFlightMessageGroup
	messageProcessorRequestedCommit bool
}

func (pp *partitionProcessor) consumerMessageChannels() []<-chan *sarama.ConsumerMessage {
	chans := make([]<-chan *sarama.ConsumerMessage, len(pp.partitionConsumers))
	for i, consumer := range pp.partitionConsumers {
		chans[i] = consumer.Messages()
	}
	return chans
}

func newPartitionProcessor(tp *TopicProcessor, mp MessageProcessor, partition int) *partitionProcessor {
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
		tp.inputTopics,
		partition,
		make(map[string][]*inFlightMessageGroup),
		false,
	}
	pp.coordinator = &partitionProcessorCoordinator{pp}
	return pp
}

func (pp *partitionProcessor) process(consumerMessage *sarama.ConsumerMessage) ([]*sarama.ProducerMessage, bool) {
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
	sender := newSender(pp, &incomingMessage)
	pp.messageProcessorRequestedCommit = false
	pp.messageProcessor.Process(incomingMessage, sender, pp.coordinator)
	inFlightMessageGroup := sender.createInFlightMessageGroup()
	pp.inFlightMessageGroups[consumerMessage.Topic] = append(
		pp.inFlightMessageGroups[consumerMessage.Topic],
		inFlightMessageGroup,
	)
	return sender.producerMessages, pp.messageProcessorRequestedCommit
}

func (pp *partitionProcessor) onProcessCompleted() {
	pp.pruneInFlightMessageGroups()
}

func (pp *partitionProcessor) pruneInFlightMessageGroups() {
	for _, topic := range pp.topicProcessor.inputTopics {
		pp.pruneInFlightMessageGroupsForTopic(topic)
	}
}

func (pp *partitionProcessor) pruneInFlightMessageGroupsForTopic(topic string) {
	for len(pp.inFlightMessageGroups[topic]) > 1 {
		headGroup := pp.inFlightMessageGroups[topic][0]
		nextGroup := pp.inFlightMessageGroups[topic][1]
		if !headGroup.allAcksAreTrue() || !nextGroup.allAcksAreTrue() {
			break
		}
		pp.inFlightMessageGroups[topic] = pp.inFlightMessageGroups[topic][1:]
	}
}

func (pp *partitionProcessor) isReadyForMessage(msg *sarama.ConsumerMessage) bool {
	maxGroups := pp.topicProcessor.config.Config.MaxInFlightMessageGroups
	return len(pp.inFlightMessageGroups[msg.Topic]) <= maxGroups
}

func (pp *partitionProcessor) onMarkOffsetsTick() {
	for _, topic := range pp.topicProcessor.inputTopics {
		pp.onMarkOffsetsTickForTopic(topic)
	}
}

func (pp *partitionProcessor) onMarkOffsetsTickForTopic(topic string) {
	var offset int64 = -1
	for len(pp.inFlightMessageGroups[topic]) > 0 {
		group := pp.inFlightMessageGroups[topic][0]
		if !group.allAcksAreTrue() {
			break
		}
		offset = group.incomingMessage.Offset
		pp.inFlightMessageGroups[topic] = pp.inFlightMessageGroups[topic][1:]
	}
	if offset != -1 {
		offsetManager := pp.offsetManagers[topic]
		offsetManager.MarkOffset(offset+1, "")
	}
}

func (pp *partitionProcessor) onProducerAck(sentMessage *sarama.ProducerMessage) {
	incomingMessage := sentMessage.Metadata.(*IncomingMessage)
	foundGroup := false
	for _, group := range pp.inFlightMessageGroups[incomingMessage.Topic] {
		if group.incomingMessage == incomingMessage {
			foundGroup = true
			foundMsg := false
			for _, inFlightMessage := range group.inFlightMessages {
				if inFlightMessage.msg == sentMessage {
					foundMsg = true
					inFlightMessage.ack = true
					break
				}
			}
			if !foundMsg {
				log.Fatal("Could not find producer message in inFlightMessageGroups")
			}
			break
		}
	}
	if !foundGroup {
		log.Fatal("Could not find group in inFlightMessageGroups")
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

func (pp *partitionProcessor) isReadyToCommit() bool {
	pp.pruneInFlightMessageGroups()
	for _, topic := range pp.inputTopics {
		if len(pp.inFlightMessageGroups[topic]) == 0 {
			continue
		} else if len(pp.inFlightMessageGroups[topic]) > 1 {
			return false
		} else {
			group := pp.inFlightMessageGroups[topic][0]
			if !group.allAcksAreTrue() {
				return false
			}
		}
	}
	return true
}

func (pp *partitionProcessor) commit() {
	for _, topic := range pp.inputTopics {
		if len(pp.inFlightMessageGroups[topic]) == 0 {
			continue
		}
		group := pp.inFlightMessageGroups[topic][0]
		offset := group.incomingMessage.Offset
		offsetManager := pp.offsetManagers[topic]
		offsetManager.MarkOffset(offset+1, "")
	}
}
