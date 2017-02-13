package kasper

import "github.com/Shopify/sarama"

// Config describes a configuration for Kasper
type Config struct {
	// The level of acknowledgement reliability needed from the broker.
	// Default: sarama.WaitForAll, wait for all in-sync replicas to commit.
	RequiredAcks sarama.RequiredAcks
	// Limit amount of message groups processed simultaneously.
	// Default: 5000.
	MaxInFlightMessageGroups int
}

// DefaultConfig creates a config that you can start with
func DefaultConfig() *Config {
	return &Config{
		RequiredAcks:             sarama.WaitForAll,
		MaxInFlightMessageGroups: 5000,
	}
}
