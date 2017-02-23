package kasper

// Counter is a incremental-only int value metric
type Counter interface {
	Inc(labelValues ...string)
	Add(value float64, labelValues ...string)
}

// Gauge is a float value metric
type Gauge interface {
	Set(value float64, labelValues ...string)
}

// MetricsProvider is used to create new metrics
type MetricsProvider interface {
	NewCounter(name string, help string, labelNames ...string) Counter
	NewGauge(name string, help string, labelNames ...string) Gauge
}
