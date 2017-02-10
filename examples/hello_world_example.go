package main

import (
	"fmt"
	"log"
	"time"
	"github.com/movio/kasper"
)

type HelloWorldExample struct{}

func (*HelloWorldExample) Process(msg kasper.IncomingMessage, sender kasper.Sender, coordinator kasper.Coordinator) {
	key := msg.Key.(string)
	value := msg.Value.(string)
	offset := msg.Offset
	topic := msg.Topic
	partition := msg.Partition
	format := "Got message: key='%s', value='%s' at offset='%d' (topic='%s', partition='%d')\n"
	fmt.Printf(format, key, value, offset, topic, partition)
}

func main() {
	config := kasper.TopicProcessorConfig{
		TopicProcessorName: "hello-world-examples",
		BrokerList:         []string{"localhost:9092"},
		InputTopics:        []kasper.Topic{"hello"},
		TopicSerdes: map[kasper.Topic]kasper.TopicSerde{
			"hello": {
				KeySerde:   kasper.NewStringSerde(),
				ValueSerde: kasper.NewStringSerde(),
			},
		},
		ContainerCount: 1,
		PartitionAssignment: map[kasper.Partition]kasper.ContainerId{
			kasper.Partition(0): kasper.ContainerId(0),
		},
		AutoMarkOffsetsInterval: 5 * time.Second,
		KasperConfig: kasper.DefaultKasperConfig(),
	}
	mkMessageProcessor := func() kasper.MessageProcessor { return &HelloWorldExample{} }
	topicProcessor := kasper.NewTopicProcessor(&config, mkMessageProcessor, kasper.ContainerId(0))
	topicProcessor.Run()
	log.Println("Running!")
	for {
		time.Sleep(2 * time.Second)
		log.Println("...")
	}
}
