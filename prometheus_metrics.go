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
	labelValues = append(labelValues, counter.provider.topicProcessorName, counter.provider.containerID)
	counter.promCounterVec.WithLabelValues(labelValues...).Inc()
}

// Add increases Prometheus CounterVec by value
func (counter *prometheusCounter) Add(value float64, labelValues ...string) {
	labelValues = append(labelValues, counter.provider.topicProcessorName, counter.provider.containerID)
	counter.promCounterVec.WithLabelValues(labelValues...).Add(value)
}

type prometheusGauge struct {
	provider     *PrometheusMetricsProvider
	promGaugeVec *prometheus.GaugeVec
}

// Set changes Prometheus GaugeVec to value
func (gauge *prometheusGauge) Set(value float64, labelValues ...string) {
	labelValues = append(labelValues, gauge.provider.topicProcessorName, gauge.provider.containerID)
	gauge.promGaugeVec.WithLabelValues(labelValues...).Set(value)
}

type prometheusSummary struct {
	provider       *PrometheusMetricsProvider
	promSummaryVec *prometheus.SummaryVec
}

func (summary *prometheusSummary) Observe(value float64, labelValues ...string) {
	labelValues = append(labelValues, summary.provider.topicProcessorName, summary.provider.containerID)
	summary.promSummaryVec.WithLabelValues(labelValues...).Observe(value)
}

// PrometheusMetricsProvider sends metrics to prometheus
// See: https://prometheus.io/
type PrometheusMetricsProvider struct {
	topicProcessorName string
	containerID        string
	Registry           *prometheus.Registry
	summaries          map[string]*prometheus.SummaryVec
	counters           map[string]*prometheus.CounterVec
	gauges             map[string]*prometheus.GaugeVec
}

// NewPrometheusMetricsProvider creates new PrometheusMetricsProvider
func NewPrometheusMetricsProvider(topicProcessorName string, containerID int) *PrometheusMetricsProvider {
	return &PrometheusMetricsProvider{
		topicProcessorName,
		strconv.Itoa(containerID),
		prometheus.NewRegistry(),
		make(map[string]*prometheus.SummaryVec),
		make(map[string]*prometheus.CounterVec),
		make(map[string]*prometheus.GaugeVec),
	}
}

// NewCounter creates new prometheus CounterVec
func (provider *PrometheusMetricsProvider) NewCounter(name string, help string, labelNames ...string) Counter {
	labelNames = append(labelNames, "topic_processor_name", "container_id")
	counterVec, found := provider.counters[name]
	if !found {
		counterVec = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kasper",
				Name:      name,
				Help:      help,
			},
			labelNames,
		)
		provider.Registry.MustRegister(counterVec)
		provider.counters[name] = counterVec
	}
	return &prometheusCounter{
		provider,
		counterVec,
	}
}

// NewGauge creates new prometheus GaugeVec
func (provider *PrometheusMetricsProvider) NewGauge(name string, help string, labelNames ...string) Gauge {
	labelNames = append(labelNames, "topic_processor_name", "container_id")
	gaugeVec, found := provider.gauges[name]
	if !found {
		gaugeVec = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "kasper",
				Name:      name,
				Help:      help,
			},
			labelNames,
		)
		provider.Registry.MustRegister(gaugeVec)
		provider.gauges[name] = gaugeVec
	}
	return &prometheusGauge{
		provider,
		gaugeVec,
	}
}

// NewSummary creates new prometheus SummaryVec
func (provider *PrometheusMetricsProvider) NewSummary(name string, help string, labelNames ...string) Summary {
	labelNames = append(labelNames, "topic_processor_name", "container_id")
	summaryVec, found := provider.summaries[name]
	if !found {
		summaryVec = prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "kasper",
				Name:      name,
				Help:      help,
			},
			labelNames,
		)
		provider.Registry.MustRegister(summaryVec)
		provider.summaries[name] = summaryVec
	}
	return &prometheusSummary{
		provider,
		summaryVec,
	}
}
