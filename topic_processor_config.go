package kasper

import (
	"fmt"
)

// TopicProcessorConfig describes a config for Kafka topic processor
type TopicProcessorConfig struct {
	// Used for logging
	TopicProcessorName string
	// Kafka Brokers list
	BrokerList []string
	// List of Kafka topics to process messages from
	InputTopics []string
	// List of topic partitions to process
	InputPartitions []int
	// Kasper config
	Config *Config
}

func (config *TopicProcessorConfig) kafkaConsumerGroup() string {
	return fmt.Sprintf("kasper-topic-processor-%s", config.TopicProcessorName)
}

func (config *TopicProcessorConfig) producerClientID() string {
	return fmt.Sprintf("kasper-topic-processor-%s", config.TopicProcessorName)
}
