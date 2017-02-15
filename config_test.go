package kasper

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	actual := DefaultConfig()
	expected := &Config{
		RequiredAcks:             sarama.WaitForAll,
		MaxInFlightMessageGroups: 5000,
		MarkOffsetsHook:          actual.MarkOffsetsHook,
	}
	assert.NotNil(t, actual.MarkOffsetsHook)
	assert.Equal(t, expected, actual)
}
