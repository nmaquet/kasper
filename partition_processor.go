package kasper

import (
	"github.com/Shopify/sarama"
	"log"
)

type outgoingMessageSender struct {
	pp               *partitionProcessor
	incomingMessage  *IncomingMessage
	producerMessages []*sarama.ProducerMessage
}

func newOutgoingMessageSender(pp *partitionProcessor, incomingMessage *IncomingMessage) *outgoingMessageSender {
	return &outgoingMessageSender{
		pp,
		incomingMessage,
		[]*sarama.ProducerMessage{},
	}
}

func (sender *outgoingMessageSender) createInFlightMessageGroup(committed bool) *inFlightMessageGroup {
	res := inFlightMessageGroup{
		incomingMessage:  sender.incomingMessage,
		inFlightMessages: nil,
		committed:        committed,
	}
	for _, msg := range sender.producerMessages {
		res.inFlightMessages = append(res.inFlightMessages, &inFlightMessage{
			msg: msg,
			ack: false,
		})
	}
	return &res
}

type inFlightMessage struct {
	msg *sarama.ProducerMessage
	ack bool
}

type inFlightMessageGroup struct {
	incomingMessage  *IncomingMessage
	inFlightMessages []*inFlightMessage
	committed        bool
}

func (group *inFlightMessageGroup) allAcksAreTrue() bool {
	if group.inFlightMessages == nil {
		return true
	}
	for _, msg := range group.inFlightMessages {
		if msg.ack == false {
			return false
		}
	}
	return true
}

func (sender *outgoingMessageSender) Send(msg OutgoingMessage) {
	topicSerde, ok := sender.pp.topicProcessor.config.TopicSerdes[msg.Topic]
	if !ok {
		log.Fatalf("Could not find Serde for topic '%s'", msg.Topic)
	}
	producerMessage := &sarama.ProducerMessage{
		Topic:     string(msg.Topic),
		Key:       sarama.ByteEncoder(topicSerde.KeySerde.Serialize(msg.Key)),
		Value:     sarama.ByteEncoder(topicSerde.ValueSerde.Serialize(msg.Value)),
		Partition: int32(msg.Partition),
		Metadata:  sender.incomingMessage,
	}
	sender.pp.producer.Input() <- producerMessage
	sender.producerMessages = append(sender.producerMessages, producerMessage)
}

type PartitionProcessorContext struct {
	// TODO
}

type MessageProcessor interface {
	Process(IncomingMessage, Sender, Coordinator)
}

type Initializer interface {
	Initialize(TopicProcessorConfig, PartitionProcessorContext)
}

type partitionProcessor struct {
	topicProcessor                 *TopicProcessor
	consumers                      []sarama.PartitionConsumer
	offsetManagers                 map[Topic]sarama.PartitionOffsetManager
	producer                       sarama.AsyncProducer
	messageProcessor               MessageProcessor
	inputTopics                    []Topic
	partition                      Partition
	inFlightMessageGroups          map[Topic][]*inFlightMessageGroup
	commitNextInFlightMessageGroup bool
}

func (pp *partitionProcessor) Commit() error {
	pp.commitNextInFlightMessageGroup = true
}

func (*partitionProcessor) Shutdown() error {
	panic("TODO: ")
}

func (pp *partitionProcessor) consumerMessageChannels() []<-chan *sarama.ConsumerMessage {
	chans := make([]<-chan *sarama.ConsumerMessage, len(pp.consumers))
	for i, consumer := range pp.consumers {
		chans[i] = consumer.Messages()
	}
	return chans
}

func mustSetupProducer(brokers []string, producerClientId string) sarama.AsyncProducer {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = producerClientId
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll /* TODO: make this configurable */

	producer, err := sarama.NewAsyncProducer(brokers, saramaConfig)
	if err != nil {
		log.Fatal(err)
	}

	return producer
}

func newPartitionProcessor(tp *TopicProcessor, mp MessageProcessor, partition Partition) *partitionProcessor {
	// FIXME store the consumer? close it?
	consumer, err := sarama.NewConsumerFromClient(tp.client)
	if err != nil {
		log.Fatal(err)
	}
	partitionConsumers := make([]sarama.PartitionConsumer, len(tp.inputTopics))
	partitionOffsetManagers := make(map[Topic]sarama.PartitionOffsetManager)
	for i, topic := range tp.inputTopics {
		pom, err := tp.offsetManager.ManagePartition(string(topic), int32(partition))
		if err != nil {
			log.Fatal(err)
		}
		newestOffset, err := tp.client.GetOffset(string(topic), int32(partition), sarama.OffsetNewest)
		if err != nil {
			log.Fatal(err)
		}
		nextOffset, _ := pom.NextOffset()
		if nextOffset > newestOffset {
			nextOffset = sarama.OffsetNewest
		}
		c, err := consumer.ConsumePartition(string(topic), int32(partition), nextOffset)
		if err != nil {
			log.Fatal(err)
		}
		partitionConsumers[i] = c
		partitionOffsetManagers[topic] = pom
	}
	producer := mustSetupProducer(tp.config.BrokerList, tp.config.producerClientId(tp.containerId))
	return &partitionProcessor{
		tp,
		partitionConsumers,
		partitionOffsetManagers,
		producer,
		mp,
		tp.inputTopics,
		partition,
		make(map[Topic][]*inFlightMessageGroup),
		false,
	}
}

func (pp *partitionProcessor) processConsumerMessage(consumerMessage *sarama.ConsumerMessage) {
	topicSerde, ok := pp.topicProcessor.config.TopicSerdes[Topic(consumerMessage.Topic)]
	if !ok {
		log.Fatalf("Could not find Serde for topic '%s'", consumerMessage.Topic)
	}
	incomingMessage := IncomingMessage{
		Topic:     Topic(consumerMessage.Topic),
		Partition: Partition(consumerMessage.Partition),
		Offset:    Offset(consumerMessage.Offset),
		Key:       topicSerde.KeySerde.Deserialize(consumerMessage.Key),
		Value:     topicSerde.ValueSerde.Deserialize(consumerMessage.Value),
		Timestamp: consumerMessage.Timestamp,
	}
	outgoingMessageSender := newOutgoingMessageSender(pp, &incomingMessage)
	pp.commitNextInFlightMessageGroup = false
	pp.messageProcessor.Process(incomingMessage, outgoingMessageSender, pp)
	inFlightMessageGroup := outgoingMessageSender.createInFlightMessageGroup(pp.commitNextInFlightMessageGroup)
	pp.inFlightMessageGroups[Topic(consumerMessage.Topic)] = append(
		pp.inFlightMessageGroups[Topic(consumerMessage.Topic)],
		inFlightMessageGroup,
	)
	pp.pruneInFlightMessageGroups()
}

func (pp *partitionProcessor) pruneInFlightMessageGroups() {
	for _, topic := range pp.topicProcessor.inputTopics {
		pp.pruneInFlightMessageGroupsForTopic(topic)
	}
}

func (pp *partitionProcessor) pruneInFlightMessageGroupsForTopic(topic Topic) {
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
	return len(pp.inFlightMessageGroups[Topic(msg.Topic)]) <= 1000 // TODO: make this configurable
}

func (pp *partitionProcessor) markOffsets() {
	for _, topic := range pp.topicProcessor.inputTopics {
		pp.markOffsetsForTopic(topic)
	}
}

func (pp *partitionProcessor) markOffsetsForTopic(topic Topic) {
	var offset Offset = -1
	for len(pp.inFlightMessageGroups[topic]) > 0 {
		group := pp.inFlightMessageGroups[topic][0]
		if !group.allAcksAreTrue() {
			break
		}
		offset = group.incomingMessage.Offset
		if group.committed {
			offsetManager := pp.offsetManagers[topic]
			offsetManager.MarkOffset(int64(offset+1), "")
		}
		pp.inFlightMessageGroups[topic] = pp.inFlightMessageGroups[topic][1:]
	}
	if offset != -1 && pp.topicProcessor.config.AutoMarkOffsetsInterval > 0 {
		offsetManager := pp.offsetManagers[topic]
		offsetManager.MarkOffset(int64(offset+1), "")
	}
}

func (pp *partitionProcessor) processProducerMessageSuccess(producerMessage *sarama.ProducerMessage) {
	incomingMessage := producerMessage.Metadata.(*IncomingMessage)
	foundGroup := false
	for _, group := range pp.inFlightMessageGroups[incomingMessage.Topic] {
		if group.incomingMessage == incomingMessage {
			foundGroup = true
			foundMsg := false
			for _, inFlightMessage := range group.inFlightMessages {
				if inFlightMessage.msg == producerMessage {
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
