package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/movio/kasper"
)

type MultipleInputTopicsExample struct{}

func (*MultipleInputTopicsExample) Process(msg kasper.IncomingMessage, sender kasper.Sender, coordinator kasper.Coordinator) {
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
		TopicProcessorName: "multiple-input-topics-example",
		BrokerList:         []string{"localhost:9092"},
		InputTopics:        []kasper.Topic{"hello", "world"},
		TopicSerdes: map[kasper.Topic]kasper.TopicSerde{
			"hello": {
				KeySerde:   kasper.NewStringSerde(),
				ValueSerde: kasper.NewStringSerde(),
			},
			"world": {
				KeySerde:   kasper.NewStringSerde(),
				ValueSerde: kasper.NewStringSerde(),
			},
		},
		ContainerCount: 1,
		PartitionAssignment: map[int]int{
			0: 0,
		},
		AutoMarkOffsetsInterval: 5 * time.Second,
		Config:                  kasper.DefaultConfig(),
	}
	makeProcessor := func() kasper.MessageProcessor { return &MultipleInputTopicsExample{} }
	topicProcessor := kasper.NewTopicProcessor(&config, makeProcessor, 0)
	topicProcessor.Start()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Topic processor is running...")
	for range signals {
		signal.Stop(signals)
		topicProcessor.Shutdown()
		break
	}
	log.Println("Topic processor shutdown complete.")
}
