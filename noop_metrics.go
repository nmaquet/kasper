package kasper

type NoopMetric struct{}

func (NoopMetric) Set(value float64, labelValues ...string) {}

func (NoopMetric) Inc(labelValues ...string) {}

type NoopMetricsProvider struct{}

func (NoopMetricsProvider) NewCounter(name string, help string, labelNames ...string) Counter {
	return &NoopMetric{}
}

func (NoopMetricsProvider) NewGauge(name string, help string, labelNames ...string) Gauge {
	return &NoopMetric{}
}
