package kasper

type noopMetric struct {
	labelCount int
}

func (m *noopMetric) Set(value float64, labelValues ...string) {}

func (m *noopMetric) Inc(labelValues ...string) {}

func (m *noopMetric) Add(value float64, labelValues ...string) {}

func (m *noopMetric) Observe(value float64, labelValues ...string) {}

type noopMetricsProvider struct{}

func (m *noopMetricsProvider) NewCounter(name string, help string, labelNames ...string) Counter {
	return &noopMetric{len(labelNames)}
}

func (m *noopMetricsProvider) NewGauge(name string, help string, labelNames ...string) Gauge {
	return &noopMetric{len(labelNames)}
}

func (m *noopMetricsProvider) NewSummary(name string, help string, labelNames ...string) Summary {
	return &noopMetric{len(labelNames)}
}
