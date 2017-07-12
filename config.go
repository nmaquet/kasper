package kasper

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

// Config contains the configuration settings for a TopicProcessor.
type Config struct {
	// Used for logging, metrics, and Kafka consumer group
	TopicProcessorName string
	// Used for consuming and producing messages
	Client sarama.Client
	// Input topics (all topics need to have the same number of partitions)
	InputTopics []string
	// Input partitions (cannot overlap between TopicProcessor instances)
	InputPartitions []int
	// Maximum number of messages processed in one go
	BatchSize int
	// Maximum amount of time spent waiting for a batch to be filled
	BatchWaitDuration time.Duration
	// Use NewBasicLogger() or any other Logger
	Logger Logger
	// Use NewPrometheus() or any other MetricsProvider
	MetricsProvider MetricsProvider
	// 15 seconds is a sensible value
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
		config.BatchWaitDuration = 5 * time.Second
	}
	if config.Logger == nil {
		config.Logger = NewBasicLogger(false)
	}
	if config.MetricsProvider == nil {
		config.MetricsProvider = &NoopMetricsProvider{}
	}
	if config.MetricsUpdateInterval == 0 {
		config.MetricsUpdateInterval = 15 * time.Second
	}
	if !config.Client.Config().Producer.Return.Successes {
		// Required by sarama.SyncProducer
		config.Client.Config().Producer.Return.Successes = true
	}
}
