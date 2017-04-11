package kasper

import (
	"time"
)

// Config describes a configuration for Kasper
type Config struct {
	// TBD
	MetricsProvider MetricsProvider
	// TBD
	MetricsUpdateInterval time.Duration
}

// DefaultConfig creates a config that you can start with
func DefaultConfig() *Config {
	return &Config{
		MetricsProvider:       &NoopMetricsProvider{},
		MetricsUpdateInterval: 5 * time.Second,
	}
}
