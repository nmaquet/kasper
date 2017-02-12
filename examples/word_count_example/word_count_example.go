package main

import (
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/movio/kasper"
)

type WordCountExample struct {
	wordCounts map[string]int
}

type WordCount struct {
	Word     string
	Count    int
	LastSeen time.Time
}

func (processor *WordCountExample) Process(msg kasper.IncomingMessage, sender kasper.Sender, coordinator kasper.Coordinator) {
	line := msg.Value.(string)
	words := strings.Split(line, " ")
	for _, word := range words {
		word = strings.ToLower(word)
		count, found := processor.wordCounts[word]
		if !found {
			count = 1
		} else {
			count++
		}
		processor.wordCounts[word] = count
		outgoingMessage := kasper.OutgoingMessage{
			Topic:     "word-counts",
			Partition: 0,
			Key:       word,
			Value:     &WordCount{word, count, time.Now()},
		}
		sender.Send(outgoingMessage)
	}
}

func main() {
	config := kasper.TopicProcessorConfig{
		TopicProcessorName: "word-count-example",
		BrokerList:         []string{"localhost:9092"},
		InputTopics:        []kasper.Topic{"words"},
		TopicSerdes: map[kasper.Topic]kasper.TopicSerde{
			"words": {
				KeySerde:   kasper.NewStringSerde(),
				ValueSerde: kasper.NewStringSerde(),
			},
			"word-counts": {
				KeySerde:   kasper.NewStringSerde(),
				ValueSerde: kasper.NewJsonSerde(&WordCount{}),
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
