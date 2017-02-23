package kasper

import "fmt"

// NoopMetric is dummy metrics
type NoopMetric struct {
	labelCount int
}

func (m *NoopMetric) checkLabelValues(labelValues ...string) {
	if len(labelValues) != m.labelCount {
		panic(fmt.Sprintf("Expected %d labels but got %d", m.labelCount, len(labelValues)))
	}
}

// Set does nothing
func (m *NoopMetric) Set(value float64, labelValues ...string) {
	m.checkLabelValues(labelValues...)
}

// Inc does nothing
func (m *NoopMetric) Inc(labelValues ...string) {
	m.checkLabelValues(labelValues...)
}

// Add does nothing
func (m *NoopMetric) Add(value float64, labelValues ...string) {
	m.checkLabelValues(labelValues...)
}

// Observe does nothing
func (m *NoopMetric) Observe(value float64, labelValues ...string) {
	m.checkLabelValues(labelValues...)
}

// NoopMetricsProvider uses dummy metrics, that are not stored anywhere.
// Kasper uses it by default, so you can replace in with real metrics provider later.
type NoopMetricsProvider struct{}

// NewCounter creates new NoopMetric
func (m *NoopMetricsProvider) NewCounter(name string, help string, labelNames ...string) Counter {
	return &NoopMetric{len(labelNames) }
}

// NewGauge creates new NoopMetric
func (m *NoopMetricsProvider) NewGauge(name string, help string, labelNames ...string) Gauge {
	return &NoopMetric{len(labelNames) }
}

// NewSummary creates new NoopMetric
func (m *NoopMetricsProvider) NewSummary(name string, help string, labelNames ...string) Summary {
	return &NoopMetric{len(labelNames) }
}
