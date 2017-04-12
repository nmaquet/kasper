package kasper

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

// Config describes a config for Kafka topic processor
type Config struct {
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
	// TBD
	BatchSize int
	// TBD
	BatchWaitDuration time.Duration
}

func (config *Config) kafkaConsumerGroup() string {
	return fmt.Sprintf("kasper-topic-processor-%s", config.TopicProcessorName)
}

func (config *Config) producerClientID() string {
	return fmt.Sprintf("kasper-topic-processor-%s", config.TopicProcessorName)
}

func (config *Config) setDefaults() {
	if config.MetricsProvider == nil {
		config.MetricsProvider = &NoopMetricsProvider{}
	}
	if config.MetricsUpdateInterval == 0 {
		config.MetricsUpdateInterval = 15 * time.Second
	}
	if config.BatchSize == 0 {
		config.BatchSize = 1000
	}
	if config.BatchWaitDuration == 0 {
		config.MetricsUpdateInterval = 5 * time.Second
	}
}
