package kasper

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/Shopify/sarama"
)

func TestDefaultConfig(t *testing.T) {
	actual := DefaultConfig()
	expected := &Config{
		RequiredAcks: sarama.WaitForAll,
		MaxInFlightMessageGroups: 5000,
	}
	assert.Equal(t, expected, actual)
}