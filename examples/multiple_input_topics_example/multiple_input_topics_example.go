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

// MultipleInputTopicsExample is Kafka message processor that shows how to read messages from muptiple Kafka topics
type MultipleInputTopicsExample struct{}

func (processor *MultipleInputTopicsExample) Process(msgs []*sarama.ConsumerMessage, sender kasper.Sender) error {
	for _, msg := range msgs {
		processor.ProcessMessage(msg)
	}
	return nil
}

// Process processes Kafka messages from topics "hello" and "world" and prints info to console
func (*MultipleInputTopicsExample) ProcessMessage(msg *sarama.ConsumerMessage) {
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
		TopicProcessorName: "multiple-input-topics-example",
		Client:             client,
		InputTopics:        []string{"hello", "world"},
		InputPartitions:    []int{0},
	}
	makeProcessor := func() kasper.MessageProcessor { return &MultipleInputTopicsExample{} }
	topicProcessor := kasper.NewTopicProcessor(&config, makeProcessor)
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
