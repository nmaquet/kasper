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

type WordCountExample struct {
	store kv.KeyValueStore
}

type WordCount struct {
	Count int `json:"count"`
}

func (processor *WordCountExample) Process(msg kasper.IncomingMessage, sender kasper.Sender, coordinator kasper.Coordinator) {
	line := msg.Value.(string)
	words := strings.Split(line, " ")
	for _, word := range words {
		var wordCount WordCount
		wordStoreKey := fmt.Sprintf("word-count/count/%s", word)
		found := processor.Get(wordStoreKey, &wordCount)
		if !found {
			wordCount.Count = 1
		} else {
			wordCount.Count++
		}
		processor.store.Put(wordStoreKey, &wordCount)
		outgoingMessage := kasper.OutgoingMessage{
			Topic:     "words-counts",
			Partition: 0,
			Key:       msg.Key,
			Value:     fmt.Sprintf("%s has been seen %d times", word, wordCount.Count),
		}
		sender.Send(outgoingMessage)
	}
}

func (processor *WordCountExample) Get(key string, value *WordCount) bool {
	found, err := processor.store.Get(key, value)
	if err != nil {
		log.Fatalf("Failed to Get(): %s", err)
	}
	return found
}

func (processor *WordCountExample) Put(key string, value *WordCount) {
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
	// store := kasper.NewElasticsearchKeyValueStore("localhost:9200")
	// store := kasper.NewInMemoryKeyValueStore(10000)
	// store, err := kv.NewCouchbaseKeyValueStore(&kv.CouchbaseConfig{
	//	Host:          "localhost",
	//	Bucket:        "default",
	//	Password:      "",
	//	DurableWrites: false,
	//	PersistTo:     0,
	//	ReplicateTo:   0,
	// })
	// if err != nil {
	//	log.Fatal(err)
	// }
	store := kv.NewRiakKeyValueStore("127.0.0.1:8087")
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
