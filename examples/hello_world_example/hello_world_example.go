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

func (processor *HelloWorldExample) Process(msgs []*sarama.ConsumerMessage, sender kasper.Sender) error {
	for _, msg := range msgs {
		processor.ProcessMessage(msg)
	}
	return nil
}

// Process processes Kafka messages from topic "hello" and prints info to console
func (*HelloWorldExample) ProcessMessage(msg *sarama.ConsumerMessage) {
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
	tp := kasper.NewTopicProcessor(&config, mkMessageProcessor)
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		log.Println("Topic processor is running...")
		for range signals {
			signal.Stop(signals)
			tp.Close()
			return
		}
	}()
	err := tp.RunLoop()
	log.Printf("Topic processor finished with err = %s\n", err)
}
