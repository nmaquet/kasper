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

func (sender *outgoingMessageSender) createInFlightMessageGroup() *inFlightMessageGroup {
	res := inFlightMessageGroup{
		incomingMessage:  sender.incomingMessage,
		inFlightMessages: nil,
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
		Topic:     msg.Topic,
		Key:       sarama.ByteEncoder(topicSerde.KeySerde.Serialize(msg.Key)),
		Value:     sarama.ByteEncoder(topicSerde.ValueSerde.Serialize(msg.Value)),
		Partition: msg.Partition,
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
	topicProcessor        *TopicProcessor
	coordinator           Coordinator
	consumers             []sarama.PartitionConsumer
	offsetManagers        map[string]sarama.PartitionOffsetManager
	producer              sarama.AsyncProducer
	messageProcessor      MessageProcessor
	inputTopics           []string
	partition             int32
	inFlightMessageGroups map[string][]*inFlightMessageGroup
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

func newPartitionProcessor(tp *TopicProcessor, mp MessageProcessor, partition int32) *partitionProcessor {
	// FIXME store the consumer? close it?
	consumer, err := sarama.NewConsumerFromClient(tp.client)
	if err != nil {
		log.Fatal(err)
	}
	partitionConsumers := make([]sarama.PartitionConsumer, len(tp.inputTopics))
	partitionOffsetManagers := make(map[string]sarama.PartitionOffsetManager)
	for i, topic := range tp.inputTopics {
		pom, err := tp.offsetManager.ManagePartition(topic, partition)
		if err != nil {
			log.Fatal(err)
		}
		newestOffset, err := tp.client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatal(err)
		}
		nextOffset, _ := pom.NextOffset()
		if nextOffset > newestOffset {
			nextOffset = sarama.OffsetNewest
		}
		c, err := consumer.ConsumePartition(topic, partition, nextOffset)
		if err != nil {
			log.Fatal(err)
		}
		partitionConsumers[i] = c
		partitionOffsetManagers[topic] = pom
	}
	var coordinator Coordinator = nil // FIXME
	producer := mustSetupProducer(tp.config.BrokerList, tp.config.producerClientId(tp.containerId))
	return &partitionProcessor{
		tp,
		coordinator,
		partitionConsumers,
		partitionOffsetManagers,
		producer,
		mp,
		tp.inputTopics,
		partition,
		make(map[string][]*inFlightMessageGroup),
	}
}

func (pp *partitionProcessor) processConsumerMessage(consumerMessage *sarama.ConsumerMessage) {
	topicSerde, ok := pp.topicProcessor.config.TopicSerdes[consumerMessage.Topic]
	if !ok {
		log.Fatalf("Could not find Serde for topic '%s'", consumerMessage.Topic)
	}
	incomingMessage := IncomingMessage{
		Topic:     consumerMessage.Topic,
		Partition: consumerMessage.Partition,
		Offset:    consumerMessage.Offset,
		Key:       topicSerde.KeySerde.Deserialize(consumerMessage.Key),
		Value:     topicSerde.ValueSerde.Deserialize(consumerMessage.Value),
		Timestamp: consumerMessage.Timestamp,
	}
	outgoingMessageSender := newOutgoingMessageSender(pp, &incomingMessage)
	pp.messageProcessor.Process(incomingMessage, outgoingMessageSender, pp.coordinator)
	inFlightMessageGroup := outgoingMessageSender.createInFlightMessageGroup()
	pp.inFlightMessageGroups[consumerMessage.Topic] = append(
		pp.inFlightMessageGroups[consumerMessage.Topic],
		inFlightMessageGroup,
	)
}

func (pp *partitionProcessor) markOffsets() {
	for _, topic := range pp.topicProcessor.inputTopics {
		pp.markOffsetsForTopic(topic)
	}
}

func (pp *partitionProcessor) markOffsetsForTopic(topic string) {
	var offset int64 = -1
	for len(pp.inFlightMessageGroups[topic]) > 0 {
		group := pp.inFlightMessageGroups[topic][0]
		if group.allAcksAreTrue() {
			offset = group.incomingMessage.Offset
			pp.inFlightMessageGroups[topic] = pp.inFlightMessageGroups[topic][1:]
		} else {
			break
		}
	}
	if offset != -1 {
		offsetManager := pp.offsetManagers[topic]
		offsetManager.MarkOffset(offset + 1, "")
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
