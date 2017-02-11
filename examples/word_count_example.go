package main

import (
	"fmt"
	"log"
	"time"
	"github.com/movio/kasper"
	"strings"
	"os"
	"os/signal"
	"syscall"
)

type WordCountExample struct {
	wordCounts map[string]int
}

func (processor *WordCountExample) Process(msg kasper.IncomingMessage, sender kasper.Sender, coordinator kasper.Coordinator) {
	line := msg.Value.(string)
	words := strings.Split(line, " ")
	for _, word := range words {
		count, found := processor.wordCounts[word]
		if !found {
			count = 1
		} else {
			count++
		}
		processor.wordCounts[word] = count
		outgoingMessage := kasper.OutgoingMessage{
			Topic:     "hello-count",
			Partition: 0,
			Key:       msg.Key,
			Value:     fmt.Sprintf("%s has been seen %d times", word, count),
		}
		sender.Send(outgoingMessage)
	}
}

func main() {
	config := kasper.TopicProcessorConfig{
		TopicProcessorName: "word-count-example",
		BrokerList:         []string{"localhost:9092"},
		InputTopics:        []kasper.Topic{"hello"},
		TopicSerdes: map[kasper.Topic]kasper.TopicSerde{
			"hello": {
				KeySerde:   kasper.NewStringSerde(),
				ValueSerde: kasper.NewStringSerde(),
			},
			"hello-count": {
				KeySerde:   kasper.NewStringSerde(),
				ValueSerde: kasper.NewStringSerde(),
			},
		},
		ContainerCount: 1,
		PartitionAssignment: map[kasper.Partition]kasper.ContainerId{
			kasper.Partition(0): kasper.ContainerId(0),
		},
		AutoMarkOffsetsInterval: 1000 * time.Millisecond,
		KasperConfig:            kasper.DefaultKasperConfig(),
	}
	mkMessageProcessor := func() kasper.MessageProcessor { return &WordCountExample{make(map[string]int)} }
	topicProcessor := kasper.NewTopicProcessor(&config, mkMessageProcessor, kasper.ContainerId(0))
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
