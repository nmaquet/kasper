package kasper

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func sortedPartitionsForContainer(c *TopicProcessorConfig, containerID int) []int {
	partitions := c.partitionsForContainer(containerID)
	sort.Ints(partitions)
	return partitions
}

func TestTopicProcessorConfig_partitionsForContainer(t *testing.T) {
	c := &TopicProcessorConfig{
		PartitionToContainerID: map[int]int{
			0: 0,
			1: 0,
			2: 0,
			3: 1,
			4: 1,
			5: 2,
		},
	}
	assert.Equal(t, []int{0, 1, 2}, sortedPartitionsForContainer(c, 0))
	assert.Equal(t, []int{3, 4}, sortedPartitionsForContainer(c, 1))
	assert.Equal(t, []int{5}, sortedPartitionsForContainer(c, 2))
	assert.Equal(t, []int{}, sortedPartitionsForContainer(c, 3))
}

func TestTopicProcessorConfig_kafkaConsumerGroup(t *testing.T) {
	c := &TopicProcessorConfig{
		TopicProcessorName: "hari-seldon",
	}
	assert.Equal(t, "kasper-topic-processor-hari-seldon", c.kafkaConsumerGroup())
}

func TestTopicProcessorConfig_producerClientID(t *testing.T) {
	c := &TopicProcessorConfig{
		TopicProcessorName: "ford-prefect",
	}
	assert.Equal(t, "kasper-topic-processor-ford-prefect-42", c.producerClientID(42))
}
