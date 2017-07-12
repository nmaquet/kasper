package kasper

type noopMetric struct {
	labelCount int
}

func (m *noopMetric) Set(value float64, labelValues ...string) {}

func (m *noopMetric) Inc(labelValues ...string) {}

func (m *noopMetric) Add(value float64, labelValues ...string) {}

func (m *noopMetric) Observe(value float64, labelValues ...string) {}

// NoopMetricsProvider is a dummy implementation of MetricsProvider that does nothing.
// Useful for testing, not recommended in production.
type NoopMetricsProvider struct{}

// NewCounter creates a new no-op Counter
func (m *NoopMetricsProvider) NewCounter(name string, help string, labelNames ...string) Counter {
	return &noopMetric{len(labelNames)}
}

// NewGauge creates a new no-op Gauge
func (m *NoopMetricsProvider) NewGauge(name string, help string, labelNames ...string) Gauge {
	return &noopMetric{len(labelNames)}
}

// NewSummary creates a new no-op Summary
func (m *NoopMetricsProvider) NewSummary(name string, help string, labelNames ...string) Summary {
	return &noopMetric{len(labelNames)}
}
