package kasper

import (
	"fmt"
	"time"
)

// TopicProcessorConfig desribes a config for Kafka topic processor
type TopicProcessorConfig struct {
	// Used for logging
	TopicProcessorName string
	// Kafka Brokers list
	BrokerList []string
	// List of Kafka topics to process messages from
	InputTopics []string
	// Mapping of topic name to key/value serdes for that topic
	TopicSerdes map[string]TopicSerde
	// Number of containers to create
	ContainerCount int
	// Mapping of partition to container to use
	PartitionToContainerID map[int]int
	// Interval used for marking offsets automatically.
	// Value <= 0 will disable the automatic marking of offsets.
	AutoMarkOffsetsInterval time.Duration
	// Kasper config
	Config *Config
}

func FairPartitionToContainerID(partitionCount, containerCount int) map[int]int {
	res := make(map[int]int)
	for i := 0; i < partitionCount; i++ {
		res[i] = i % containerCount
	}
	return res
}

func (config *TopicProcessorConfig) partitionsForContainer(containerID int) []int {
	partitions := []int{}
	for partition, partitionContainerID := range config.PartitionToContainerID {
		if containerID == partitionContainerID {
			partitions = append(partitions, partition)
		}
	}
	return partitions
}

func (config *TopicProcessorConfig) kafkaConsumerGroup() string {
	return fmt.Sprintf("kasper-topic-processor-%s", config.TopicProcessorName)
}

func (config *TopicProcessorConfig) producerClientID(containerID int) string {
	return fmt.Sprintf("kasper-topic-processor-%s-%d", config.TopicProcessorName, containerID)
}

func (config *TopicProcessorConfig) markOffsetsAutomatically() bool {
	return config.AutoMarkOffsetsInterval > 0
}

func (config *TopicProcessorConfig) markOffsetsManually() bool {
	return config.AutoMarkOffsetsInterval <= 0
}
