package kasper

import "github.com/Shopify/sarama"

// Config describes a configuration for Kasper
type Config struct {
	RequiredAcks             sarama.RequiredAcks
	MaxInFlightMessageGroups int
}

// DefaultConfig creates a config that you can start with
func DefaultConfig() *Config {
	return &Config{
		RequiredAcks:             sarama.WaitForAll,
		MaxInFlightMessageGroups: 5000,
	}
}
