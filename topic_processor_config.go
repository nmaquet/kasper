package kasper

import (
	"fmt"
	"time"
)

type Container struct {
	ContainerId int
}

type TopicProcessorConfig struct {
	TopicProcessorName      string
	BrokerList              []string
	InputTopics             []string
	TopicSerdes             map[string]TopicSerde
	ContainerCount          int
	PartitionAssignment     map[int32]Container
	AutoMarkOffsetsInterval time.Duration /* a value <= 0 will disable the automatic marking of offsets */
}

func (config *TopicProcessorConfig) partitionsForContainer(containerId int) []int32 {
	var partitions []int32
	for partition, container := range config.PartitionAssignment {
		if container.ContainerId == containerId {
			partitions = append(partitions, partition)
		}
	}
	return partitions
}

func (config *TopicProcessorConfig) kafkaConsumerGroup() string {
	return fmt.Sprintf("kasper-topic-processor-%s", config.TopicProcessorName)
}

func (config *TopicProcessorConfig) producerClientId(containerId int) string {
	return fmt.Sprintf("kasper-topic-processor-%s-%d", config.TopicProcessorName, containerId)
}
