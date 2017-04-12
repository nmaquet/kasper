package kasper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopicProcessorConfig_kafkaConsumerGroup(t *testing.T) {
	c := &Config{
		TopicProcessorName: "hari-seldon",
	}
	assert.Equal(t, "kasper-topic-processor-hari-seldon", c.kafkaConsumerGroup())
}

func TestTopicProcessorConfig_producerClientID(t *testing.T) {
	c := &Config{
		TopicProcessorName: "ford-prefect",
	}
	assert.Equal(t, "kasper-topic-processor-ford-prefect", c.producerClientID())
}
