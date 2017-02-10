package main

import (
	"fmt"
	"log"
	"time"
	"github.com/movio/kasper"
)

type KeyValueStoreExample struct {
	store kasper.KeyValueStore
}

type WordCount struct {
	Count int `json:"count"`
}

func (processor *KeyValueStoreExample) Process(msg kasper.IncomingMessage, sender kasper.Sender, coordinator kasper.Coordinator) {
	word := msg.Value.(string)
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
		Topic:     "hello-count",
		Partition: 0,
		Key:       msg.Key,
		Value:     fmt.Sprintf("%s has been seen %d times", word, wordCount.Count),
	}
	sender.Send(outgoingMessage)
}

func (processor *KeyValueStoreExample) Get(key string, value *WordCount) bool {
	found, err := processor.store.Get(key, value)
	if err != nil {
		log.Fatalf("Failed to Get(): %s", err)
	}
	return found
}

func (processor *KeyValueStoreExample) Put(key string, value *WordCount) {
	err := processor.store.Put(key, value)
	if err != nil {
		log.Fatalf("Failed to Put(): %s", err)
	}
}

func main() {
	config := kasper.TopicProcessorConfig{
		TopicProcessorName: "key-value-store-example",
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
		AutoMarkOffsetsInterval: 100 * time.Millisecond,
		KasperConfig: kasper.DefaultKasperConfig(),
	}
	// store := kasper.NewElasticsearchKeyValueStore("localhost:9200")
	store := kasper.NewInMemoryKeyValueStore(100)
	mkMessageProcessor := func() kasper.MessageProcessor { return &KeyValueStoreExample{store} }
	topicProcessor := kasper.NewTopicProcessor(&config, mkMessageProcessor, kasper.ContainerId(0))
	topicProcessor.Run()
	log.Println("Running!")
	for {
		time.Sleep(2 * time.Second)
		log.Println("...")
	}
}
