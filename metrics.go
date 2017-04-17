package kasper

// Counter is a single float metric that can be incremented by one or added to.
type Counter interface {
	Inc(labelValues ...string)
	Add(value float64, labelValues ...string)
}

// Gauge is a single float metric that can be set to a specific value.
type Gauge interface {
	Set(value float64, labelValues ...string)
}

// Summary is a float value metric that provides a history of observations.
type Summary interface {
	Observe(value float64, labelValues ...string)
}

// MetricsProvider is a facility to create metrics instances.
type MetricsProvider interface {
	NewCounter(name string, help string, labelNames ...string) Counter
	NewGauge(name string, help string, labelNames ...string) Gauge
	NewSummary(name string, help string, labelNames ...string) Summary
}
