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
	// TBD
	MetricsProvider MetricsProvider
	// TBD
	MetricsUpdateInterval time.Duration
}

// DefaultConfig creates a config that you can start with
func DefaultConfig() *Config {
	return &Config{
		RequiredAcks:             sarama.WaitForAll,
		MetricsProvider:          &NoopMetricsProvider{},
		MetricsUpdateInterval:    5 * time.Second,
	}
}
