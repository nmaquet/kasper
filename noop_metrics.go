package kasper

// NoopMetric is dummy metrics
type NoopMetric struct{}

// Set does nothing
func (NoopMetric) Set(value float64, labelValues ...string) {}

// Inc does nothing
func (NoopMetric) Inc(labelValues ...string) {}

// Add does nothing
func (NoopMetric) Add(value float64, labelValues ...string) {}

// NoopMetricsProvider uses dummy metrics, that are not stored anywhere.
// Kasper uses it by default, so you can replace in with real metrics provider later.
type NoopMetricsProvider struct{}

// NewCounter creates new NoopMetric
func (NoopMetricsProvider) NewCounter(name string, help string, labelNames ...string) Counter {
	return &NoopMetric{}
}

// NewGauge creates new NoopMetric
func (NoopMetricsProvider) NewGauge(name string, help string, labelNames ...string) Gauge {
	return &NoopMetric{}
}
