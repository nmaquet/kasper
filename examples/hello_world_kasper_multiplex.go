package main

import (
	"fmt"
	"log"
	"time"
	"github.com/movio/kasper"
)

type HelloWorldProcessorMultiplex struct{}

func (*HelloWorldProcessorMultiplex) Process(msg kasper.IncomingMessage, sender kasper.Sender, coordinator kasper.Coordinator) {
	key := msg.Key.(string)
	value := msg.Value.(string)
	offset := msg.Offset
	topic := msg.Topic
	partition := msg.Partition
	format := "Got message: key='%s', value='%s' at offset='%s' (topic='%s', partition='%d')\n"
	fmt.Printf(format, key, value, offset, topic, partition)
}

func main() {
	config := kasper.TopicProcessorConfig{
		BrokerList:  []string{"localhost:9092"},
		InputTopics: []string{"hello", "world"},
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
	}
	makeProcessor := func() kasper.MessageProcessor { return &HelloWorldProcessorMultiplex{} }
	containerId := 0
	topicProcessor := kasper.NewTopicProcessor(&config, makeProcessor, containerId)
	topicProcessor.Run()
	log.Println("Running!")
	for {
		time.Sleep(2 * time.Second)
		log.Println("...")
	}
}
