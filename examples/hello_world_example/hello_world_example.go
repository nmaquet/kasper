package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/movio/kasper"
)

// HelloWorldExample is Kafka message processor that shows how to read messages from Kafka topic
type HelloWorldExample struct{}

// Process processes Kafka messages from topic "hello" and prints info to console
func (*HelloWorldExample) Process(msg *sarama.ConsumerMessage, sender kasper.Sender, coordinator kasper.Coordinator) {
	key := string(msg.Key)
	value := string(msg.Value)
	offset := msg.Offset
	topic := msg.Topic
	partition := msg.Partition
	format := "Got message: key='%s', value='%s' at offset='%d' (topic='%s', partition='%d')\n"
	fmt.Printf(format, key, value, offset, topic, partition)
}

func main() {
	client, _ := sarama.NewClient([]string{"localhost:9092"}, sarama.NewConfig())
	config := kasper.Config{
		TopicProcessorName: "hello-world-example",
		Client:             client,
		InputTopics:        []string{"hello"},
		InputPartitions:    []int{0},
	}
	mkMessageProcessor := func() kasper.MessageProcessor { return &HelloWorldExample{} }
	topicProcessor := kasper.NewTopicProcessor(&config, mkMessageProcessor)
	topicProcessor.Start()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Topic processor is running...")
	for range signals {
		signal.Stop(signals)
		topicProcessor.Close()
		break
	}
	log.Println("Topic processor closed.")
}
