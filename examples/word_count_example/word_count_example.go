package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/movio/kasper"
)

// WordCountExample is message processor that shows how to use key-value store in processing Kafka messages
// and outputs results to topic "word-counts"
type WordCountExample struct {
	store kasper.Store
}

func (processor *WordCountExample) Process(msgs []*sarama.ConsumerMessage, sender kasper.Sender) error {
	for _, msg := range msgs {
		processor.ProcessMessage(msg, sender)
	}
	return nil
}

// Process processes Kafka messages from topic "words" and outputs each word with counter to "word-counts" topic
func (processor *WordCountExample) ProcessMessage(msg *sarama.ConsumerMessage, sender kasper.Sender) {
	line := string(msg.Value)
	words := strings.Split(line, " ")
	for _, word := range words {
		wordStoreKey := fmt.Sprintf("word-count/count/%s", word)
		wordCount := processor.get(wordStoreKey)
		processor.put(wordStoreKey, wordCount+1)
		outgoingMessage := &sarama.ProducerMessage{
			Topic:     "word-counts",
			Partition: 0,
			Key:       sarama.ByteEncoder(msg.Key),
			Value:     sarama.ByteEncoder([]byte(fmt.Sprintf("%s has been seen %d times", word, wordCount))),
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
	client, _ := sarama.NewClient([]string{"localhost:9092"}, sarama.NewConfig())
	config := kasper.Config{
		TopicProcessorName: "key-value-store-example",
		Client:             client,
		InputTopics:        []string{"words"},
		InputPartitions:    []int{0},
	}
	store := kasper.NewMap(10000)
	messageProcessors := map[int]kasper.MessageProcessor{0: &WordCountExample{store}}
	tp := kasper.NewTopicProcessor(&config, messageProcessors)
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		log.Println("Topic processor is running...")
		for range signals {
			signal.Stop(signals)
			tp.Close()
			break
		}
	}()
	err := tp.RunLoop()
	log.Printf("Topic processor finished with err = %s\n", err)
}
