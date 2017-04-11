package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/movio/kasper"
	"strconv"
)

// WordCountExample is message processor that shows how to use key-value store in processing Kafka messages
// and outputs results to topic "word-counts"
type WordCountExample struct {
	store kasper.KeyValueStore
}

// Process processes Kafka messages from topic "words" and outputs each word with counter to "word-counts" topic
func (processor *WordCountExample) Process(msg kasper.IncomingMessage, sender kasper.Sender, coordinator kasper.Coordinator) {
	line := string(msg.Value)
	words := strings.Split(line, " ")
	for _, word := range words {
		wordStoreKey := fmt.Sprintf("word-count/count/%s", word)
		wordCount := processor.get(wordStoreKey)
		processor.put(wordStoreKey, wordCount + 1)
		outgoingMessage := kasper.OutgoingMessage{
			Topic:     "word-counts",
			Partition: 0,
			Key:       msg.Key,
			Value:     []byte(fmt.Sprintf("%s has been seen %d times", word, wordCount)),
		}
		sender.Send(outgoingMessage)
	}
}

func (processor *WordCountExample) get(key string) int {
	data, err := processor.store.Get(key)
	if err != nil {
		panic(err)
	}
	if data == nil {
		return 0
	}
	count, err := strconv.Atoi(string(data))
	if err != nil {
		panic(err)
	}
	return count
}

func (processor *WordCountExample) put(key string, count int) {
	err := processor.store.Put(key, []byte(strconv.Itoa(count)))
	if err != nil {
		panic(err)
	}
}

func main() {
	config := kasper.TopicProcessorConfig{
		TopicProcessorName: "key-value-store-example",
		BrokerList:         []string{"localhost:9092"},
		InputTopics:        []string{"words"},
		InputPartitions:    []int{0},
		Config: kasper.DefaultConfig(),
	}
	store := kasper.NewInMemoryKeyValueStore(10000)
	mkMessageProcessor := func() kasper.MessageProcessor { return &WordCountExample{store} }
	topicProcessor := kasper.NewTopicProcessor(&config, mkMessageProcessor)
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
