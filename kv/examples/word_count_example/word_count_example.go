package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/movio/kasper"
	"github.com/movio/kasper/kv"
)

// WordCountExample is message processor that shows how to use key-value store in processing Kafka messages
// and outputs results to topic "word-counts"
type WordCountExample struct {
	store kv.KeyValueStore
}

// WordCount describes Kafka outgoing message
type WordCount struct {
	Count int `json:"count"`
}

// Process processes Kafka messages from topic "words" and outputs each word with counter to "word-counts" topic
func (processor *WordCountExample) Process(msg kasper.IncomingMessage, sender kasper.Sender, coordinator kasper.Coordinator) {
	line := msg.Value.(string)
	words := strings.Split(line, " ")
	for _, word := range words {
		wordStoreKey := fmt.Sprintf("word-count/count/%s", word)
		wordCount := processor.get(wordStoreKey)
		if wordCount == nil {
			wordCount = &WordCount{1}
		} else {
			wordCount.Count++
		}
		processor.put(wordStoreKey, wordCount)
		outgoingMessage := kasper.OutgoingMessage{
			Topic:     "word-counts",
			Partition: 0,
			Key:       msg.Key,
			Value:     fmt.Sprintf("%s has been seen %d times", word, wordCount.Count),
		}
		sender.Send(outgoingMessage)
	}
}

func (processor *WordCountExample) get(key string) *WordCount {
	wordCount, err := processor.store.Get(key)
	if err != nil {
		log.Fatalf("Failed to Get(): %s", err)
	}
	return wordCount.(*WordCount)
}

func (processor *WordCountExample) put(key string, value *WordCount) {
	err := processor.store.Put(key, value)
	if err != nil {
		log.Fatalf("Failed to Put(): %s", err)
	}
}

func main() {
	config := kasper.TopicProcessorConfig{
		TopicProcessorName: "key-value-store-example",
		BrokerList:         []string{"localhost:9092"},
		InputTopics:        []string{"words"},
		TopicSerdes: map[string]kasper.TopicSerde{
			"words": {
				KeySerde:   kasper.NewStringSerde(),
				ValueSerde: kasper.NewStringSerde(),
			},
			"word-counts": {
				KeySerde:   kasper.NewStringSerde(),
				ValueSerde: kasper.NewStringSerde(),
			},
		},
		ContainerCount: 1,
		PartitionToContainerID: map[int]int{
			0: 0,
		},
		AutoMarkOffsetsInterval: 1000 * time.Millisecond,
		Config:                  kasper.DefaultConfig(),
	}
	// store := kv.NewESKeyValueStore("localhost:9200", &WordCount{})
	// store := kv.NewInMemoryKeyValueStore(10000, &WordCount{})
	// store, err := kv.NewCouchbaseKeyValueStore(&kv.CouchbaseConfig{
	//	Host:          "localhost",
	//	Bucket:        "default",
	//	Password:      "",
	//	DurableWrites: false,
	//	PersistTo:     0,
	//	ReplicateTo:   0,
	// }, &WordCount{})
	// if err != nil {
	//	log.Fatal(err)
	// }
	// store := kv.NewRiakKeyValueStore("127.0.0.1:8087", &WordCount{})
	store, err := kv.NewBoltKeyValueStore("/tmp/db.bolt", &WordCount{})
	if err != nil {
		log.Fatalf("Could not create bolt DB: %s", err)
	}
	mkMessageProcessor := func() kasper.MessageProcessor { return &WordCountExample{store} }
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
