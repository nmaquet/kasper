package kasper

import "github.com/Shopify/sarama"

type Config struct {
	RequiredAcks             sarama.RequiredAcks
	MaxInFlightMessageGroups int
}

func DefaultConfig() *Config {
	return &Config{
		RequiredAcks:             sarama.WaitForAll,
		MaxInFlightMessageGroups: 5000,
	}
}
