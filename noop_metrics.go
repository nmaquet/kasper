package kasper

import "fmt"

// noopMetric is dummy metrics
type noopMetric struct {
	labelCount int
}

func (m *noopMetric) checkLabelValues(labelValues ...string) {
	if len(labelValues) != m.labelCount {
		logger.Panic(fmt.Sprintf("Expected %d labels but got %d", m.labelCount, len(labelValues)))
	}
}

// Set does nothing
func (m *noopMetric) Set(value float64, labelValues ...string) {
	m.checkLabelValues(labelValues...)
}

// Inc does nothing
func (m *noopMetric) Inc(labelValues ...string) {
	m.checkLabelValues(labelValues...)
}

// Add does nothing
func (m *noopMetric) Add(value float64, labelValues ...string) {
	m.checkLabelValues(labelValues...)
}

// Observe does nothing
func (m *noopMetric) Observe(value float64, labelValues ...string) {
	m.checkLabelValues(labelValues...)
}

// noopMetricsProvider uses dummy metrics, that are not stored anywhere.
// Kasper uses it by default, so you can replace in with real metrics provider later.
type noopMetricsProvider struct{}

// NewCounter creates new noopMetric
func (m *noopMetricsProvider) NewCounter(name string, help string, labelNames ...string) Counter {
	return &noopMetric{len(labelNames)}
}

// NewGauge creates new noopMetric
func (m *noopMetricsProvider) NewGauge(name string, help string, labelNames ...string) Gauge {
	return &noopMetric{len(labelNames)}
}

// NewSummary creates new noopMetric
func (m *noopMetricsProvider) NewSummary(name string, help string, labelNames ...string) Summary {
	return &noopMetric{len(labelNames)}
}
