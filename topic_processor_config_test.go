package kasper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
	assert.Equal(t, "kasper-topic-processor-ford-prefect", c.producerClientID())
}
