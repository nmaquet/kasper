package main

import (
	"fmt"
	"log"
	"time"
	"github.com/movio/kasper"
)

type HelloWorldProcessor struct{}

func (*HelloWorldProcessor) Process(msg kasper.IncomingMessage, sender kasper.Sender, coordinator kasper.Coordinator) {
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
		InputTopics: []string{"hello"},
	}
	mkMessageProcessor := func() kasper.MessageProcessor { return &HelloWorldProcessor{} }
	topicProcessor := kasper.NewTopicProcessor(&config, mkMessageProcessor, kasper.NewStringSerde(), kasper.NewStringSerde())
	topicProcessor.Run()
	log.Println("Running!")
	for {
		time.Sleep(2 * time.Second)
		log.Println("...")
	}
}
