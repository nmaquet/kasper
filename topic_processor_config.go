package kasper

import (
	"fmt"
	"time"
)

type TopicProcessorConfig struct {
	TopicProcessorName      string
	BrokerList              []string
	InputTopics             []Topic
	TopicSerdes             map[Topic]TopicSerde
	ContainerCount          int
	PartitionAssignment     map[int]int
	AutoMarkOffsetsInterval time.Duration /* a value <= 0 will disable the automatic marking of offsets */
	Config                  *Config
}

func (config *TopicProcessorConfig) partitionsForContainer(containerID int) []int {
	var partitions []int
	for partition, partitionContainerID := range config.PartitionAssignment {
		if containerID == partitionContainerID {
			partitions = append(partitions, partition)
		}
	}
	return partitions
}

func (config *TopicProcessorConfig) kafkaConsumerGroup() string {
	return fmt.Sprintf("kasper-topic-processor-%s", config.TopicProcessorName)
}

func (config *TopicProcessorConfig) producerClientId(containerID int) string {
	return fmt.Sprintf("kasper-topic-processor-%s-%d", config.TopicProcessorName, containerID)
}

func (config *TopicProcessorConfig) markOffsetsAutomatically() bool {
	return config.AutoMarkOffsetsInterval > 0
}

func (config *TopicProcessorConfig) markOffsetsManually() bool {
	return config.AutoMarkOffsetsInterval <= 0
}
