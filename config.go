package kasper

import (
	"time"

	"github.com/Shopify/sarama"
)

// Config describes a configuration for Kasper
type Config struct {
	// The level of acknowledgement reliability needed from the broker.
	// Default: sarama.WaitForAll, wait for all in-sync replicas to commit.
	RequiredAcks sarama.RequiredAcks
	// MarkOffsetsHook is called by Kasper prior to marking offsets when automatic offset marking is enabled.
	// This hook can be used to synchronously flush output buffers or external storage before marking offsets.
	// Default: a no-op funcMetrics
	MarkOffsetsHook func()
	// TBD
	MetricsProvider Provider
	// TBD
	MetricsUpdateInterval time.Duration
}

// DefaultConfig creates a config that you can start with
func DefaultConfig() *Config {
	return &Config{
		RequiredAcks:             sarama.WaitForAll,
		MarkOffsetsHook:          func() {},
		MetricsProvider:          &NoopMetricsProvider{},
		MetricsUpdateInterval:    5 * time.Second,
	}
}
