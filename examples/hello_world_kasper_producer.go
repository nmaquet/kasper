package main

import (
	"fmt"
	"log"
	"time"
	"github.com/movio/kasper"
)

type HelloWorldProducerProcessor struct{}

func (*HelloWorldProducerProcessor) Process(msg kasper.IncomingMessage, sender kasper.Sender, coordinator kasper.Coordinator) {
	key := msg.Key.(string)
	value := msg.Value.(string)
	offset := msg.Offset
	topic := msg.Topic
	partition := msg.Partition
	format := "Got message: key='%s', value='%s' at offset='%d' (topic='%s', partition='%d')\n"
	fmt.Printf(format, key, value, offset, topic, partition)
	outgoingMessage := kasper.OutgoingMessage{
		Topic:     "world",
		Partition: 0,
		Key:       msg.Key,
		Value:     fmt.Sprintf("Hello %s", msg.Value),
	}
	sender.Send(outgoingMessage)
}

func main() {
	config := kasper.TopicProcessorConfig{
		TopicProcessorName: "hello-world-producer-kasper",
		BrokerList:         []string{"localhost:9092"},
		InputTopics:        []string{"hello"},
		TopicSerdes: map[string]kasper.TopicSerde{
			"hello": {
				KeySerde:   kasper.NewStringSerde(),
				ValueSerde: kasper.NewStringSerde(),
			},
			"world": {
				KeySerde:   kasper.NewStringSerde(),
				ValueSerde: kasper.NewStringSerde(),
			},
		},
		ContainerCount:      1,
		PartitionAssignment: map[int32]kasper.Container{0: {ContainerId: 0}},
		AutoMarkOffsetsInterval: 5 * time.Second,
	}
	mkMessageProcessor := func() kasper.MessageProcessor { return &HelloWorldProducerProcessor{} }
	containerId := 0
	topicProcessor := kasper.NewTopicProcessor(&config, mkMessageProcessor, containerId)
	topicProcessor.Run()
	log.Println("Running!")
	for {
		time.Sleep(2 * time.Second)
		log.Println("...")
	}
}
