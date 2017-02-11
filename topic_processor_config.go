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
	PartitionAssignment     map[Partition]ContainerId
	AutoMarkOffsetsInterval time.Duration /* a value <= 0 will disable the automatic marking of offsets */
	KasperConfig            *KasperConfig
}

func (config *TopicProcessorConfig) partitionsForContainer(cid ContainerId) []Partition {
	var partitions []Partition
	for partition, containerId := range config.PartitionAssignment {
		if containerId == containerId {
			partitions = append(partitions, partition)
		}
	}
	return partitions
}

func (config *TopicProcessorConfig) kafkaConsumerGroup() string {
	return fmt.Sprintf("kasper-topic-processor-%s", config.TopicProcessorName)
}

func (config *TopicProcessorConfig) producerClientId(cid ContainerId) string {
	return fmt.Sprintf("kasper-topic-processor-%s-%d", config.TopicProcessorName, cid)
}

func (config *TopicProcessorConfig) markOffsetsAutomatically() bool {
	return config.AutoMarkOffsetsInterval > 0
}

func (config *TopicProcessorConfig) markOffsetsManually() bool {
	return config.AutoMarkOffsetsInterval <= 0
}