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

// ProducerExample is Kafka message processor that shows how to write messages to Kafka topics
type ProducerExample struct{}

func (processor *ProducerExample) Process(msgs []*sarama.ConsumerMessage, sender kasper.Sender, coordinator kasper.Coordinator) {
	for _, msg := range msgs {
		processor.ProcessMessage(msg, sender)
	}
}

// Process processes Kafka messages from topics "hello" and "world" and publish outgoing messages to "world" topi
func (*ProducerExample) ProcessMessage(msg *sarama.ConsumerMessage, sender kasper.Sender) {
	key := string(msg.Key)
	value := string(msg.Value)
	offset := msg.Offset
	topic := msg.Topic
	partition := msg.Partition
	format := "Got message: key='%s', value='%s' at offset='%d' (topic='%s', partition='%d')\n"
	fmt.Printf(format, key, value, offset, topic, partition)
	outgoingMessage := &sarama.ProducerMessage{
		Topic:     "world",
		Partition: 0,
		Key:       sarama.ByteEncoder(msg.Key),
		Value:     sarama.ByteEncoder([]byte(fmt.Sprintf("Hello %s", value))),
	}
	sender.Send(outgoingMessage)
}

func main() {
	client, _ := sarama.NewClient([]string{"localhost:9092"}, sarama.NewConfig())
	config := kasper.Config{
		TopicProcessorName: "producer-example",
		Client:             client,
		InputTopics:        []string{"hello", "world"},
		InputPartitions:    []int{0},
	}
	mkMessageProcessor := func() kasper.MessageProcessor { return &ProducerExample{} }
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
