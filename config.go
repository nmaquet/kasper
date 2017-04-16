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
	// TBD
	BatchSize int
	// TBD
	BatchWaitDuration time.Duration
	// TBD
	Logger Logger
	// TDB
	MetricsProvider MetricsProvider
	// TBD
	MetricsUpdateInterval time.Duration
}

func (config *Config) kafkaConsumerGroup() string {
	return fmt.Sprintf("kasper-topic-processor-%s", config.TopicProcessorName)
}

func (config *Config) producerClientID() string {
	return fmt.Sprintf("kasper-topic-processor-%s", config.TopicProcessorName)
}

func (config *Config) setDefaults() {
	if config.BatchSize == 0 {
		config.BatchSize = 1000
	}
	if config.BatchWaitDuration == 0 {
		config.MetricsUpdateInterval = 5 * time.Second
	}
	if config.Logger == nil {
		config.Logger = NewBasicLogger(false)
	}
	if config.MetricsProvider == nil {
		config.MetricsProvider = &noopMetricsProvider{}
	}
	if config.MetricsUpdateInterval == 0 {
		config.MetricsUpdateInterval = 15 * time.Second
	}
}
