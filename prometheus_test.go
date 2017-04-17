package kasper

import "testing"

func TestNewPrometheusMetricsProvider(t *testing.T) {
	provider := NewPrometheus("test")
	gauge := provider.NewGauge("test_gauge", "A test gauge", "label1", "label2")
	counter := provider.NewCounter("test_counter", "A test counter", "label1", "label2")
	summary := provider.NewSummary("test_summary", "A test summary", "label1", "label2")

	gauge.Set(42, "value1", "value2")
	counter.Inc("value1", "value2")
	counter.Add(1, "value1", "value2")
	summary.Observe(42, "value1", "value2")
}
