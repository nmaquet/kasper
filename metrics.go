package kasper

type Counter interface {
	Inc(labelValues...string)
}

type Gauge interface {
	Set(value float64, labelValues...string)
}

type MetricsProvider interface {
	NewCounter(name string, help string, labelNames ...string) Counter
	NewGauge(name string, help string, labelNames ...string) Gauge
}
