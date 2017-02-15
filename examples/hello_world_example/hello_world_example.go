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

// HelloWorldExample is Kafka message processor that shows how to read messages from Kafka topic
type HelloWorldExample struct{}

// Process processes Kafka messages from topic "hello" and prints info to console
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
		TopicProcessorName: "hello-world-example",
		BrokerList:         []string{"localhost:9092"},
		InputTopics:        []string{"hello"},
		TopicSerdes: map[string]kasper.TopicSerde{
			"hello": {
				KeySerde:   kasper.NewStringSerde(),
				ValueSerde: kasper.NewStringSerde(),
			},
		},
		ContainerCount: 1,
		PartitionToContainerID: kasper.FairPartitionToContainerID(1, 1),
		AutoMarkOffsetsInterval: 5 * time.Second,
		Config:                  kasper.DefaultConfig(),
	}
	mkMessageProcessor := func() kasper.MessageProcessor { return &HelloWorldExample{} }
	topicProcessor := kasper.NewTopicProcessor(&config, mkMessageProcessor, 0)
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
