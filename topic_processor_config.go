package kasper

type Container struct {
	ContainerId int
}

type TopicProcessorConfig struct {
	BrokerList          []string
	InputTopics         []string
	TopicSerdes         map[string]TopicSerde
	ContainerCount      int
	PartitionAssignment map[int32]Container
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
