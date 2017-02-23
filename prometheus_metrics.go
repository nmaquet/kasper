package kasper

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

type prometheusCounter struct {
	provider       *PrometheusMetricsProvider
	promCounterVec *prometheus.CounterVec
}

// Inc increments Prometheus CounterVec value
func (counter *prometheusCounter) Inc(labelValues ...string) {
	labelValues = append(labelValues, counter.provider.tpConfig.TopicProcessorName, counter.provider.containerID)
	counter.promCounterVec.WithLabelValues(labelValues...).Inc()
}

// Add increases Prometheus CounterVec by value
func (counter *prometheusCounter) Add(value float64, labelValues ...string) {
	labelValues = append(labelValues, counter.provider.tpConfig.TopicProcessorName, counter.provider.containerID)
	counter.promCounterVec.WithLabelValues(labelValues...).Add(value)
}

type prometheusGauge struct {
	provider     *PrometheusMetricsProvider
	promGaugeVec *prometheus.GaugeVec
}

// Set changes Prometheus GaugeVec to value
func (gauge *prometheusGauge) Set(value float64, labelValues ...string) {
	labelValues = append(labelValues, gauge.provider.tpConfig.TopicProcessorName, gauge.provider.containerID)
	gauge.promGaugeVec.WithLabelValues(labelValues...).Set(value)
}

// PrometheusMetricsProvider sends metrics to prometheus
// See: https://prometheus.io/
type PrometheusMetricsProvider struct {
	tpConfig    *TopicProcessorConfig
	containerID string
	Registry    *prometheus.Registry
}

// NewPrometheusMetricsProvider creates new PrometheusMetricsProvider
func NewPrometheusMetricsProvider(tpConfig *TopicProcessorConfig, containerID int) *PrometheusMetricsProvider {
	return &PrometheusMetricsProvider{
		tpConfig,
		strconv.Itoa(containerID),
		prometheus.NewRegistry(),
	}
}

// NewCounter creates new prometheus CounterVec
func (provider *PrometheusMetricsProvider) NewCounter(name string, help string, labelNames ...string) Counter {
	labelNames = append(labelNames, "topic_processor_name", "container_id")
	counterVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "kasper",
			Name:      name,
			Help:      help,
		},
		labelNames,
	)
	provider.Registry.MustRegister(counterVec)
	return &prometheusCounter{
		provider,
		counterVec,
	}
}

// NewGauge new prometheus GaugeVec
func (provider *PrometheusMetricsProvider) NewGauge(name string, help string, labelNames ...string) Gauge {
	labelNames = append(labelNames, "topic_processor_name", "container_id")
	gaugeVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kasper",
			Name:      name,
			Help:      help,
		},
		labelNames,
	)
	provider.Registry.MustRegister(gaugeVec)
	return &prometheusGauge{
		provider,
		gaugeVec,
	}
}
