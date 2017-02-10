package kasper

import "github.com/Shopify/sarama"

type KasperConfig struct {
	RequiredAcks             sarama.RequiredAcks
	MaxInFlightMessageGroups int
}

func DefaultKasperConfig() (*KasperConfig) {
	return &KasperConfig{
		RequiredAcks:             sarama.WaitForAll,
		MaxInFlightMessageGroups: 5000,
	}
}
