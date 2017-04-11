package kasper

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

// TopicProcessorConfig describes a config for Kafka topic processor
type TopicProcessorConfig struct {
	// Used for logging
	TopicProcessorName string
	// Kafka Brokers list
	Client sarama.Client
	// List of Kafka topics to process messages from
	InputTopics []string
	// List of topic partitions to process
	InputPartitions []int
	// TDB
	MetricsProvider MetricsProvider
	// TBD
	MetricsUpdateInterval time.Duration
}

func (config *TopicProcessorConfig) kafkaConsumerGroup() string {
	return fmt.Sprintf("kasper-topic-processor-%s", config.TopicProcessorName)
}

func (config *TopicProcessorConfig) producerClientID() string {
	return fmt.Sprintf("kasper-topic-processor-%s", config.TopicProcessorName)
}

func (config *TopicProcessorConfig) SetDefaults() {
	if config.MetricsProvider == nil {
		config.MetricsProvider = &NoopMetricsProvider{}
	}
	if config.MetricsUpdateInterval == 0 {
		config.MetricsUpdateInterval = 15 * time.Second
	}
}
