package kasper

import (
	"github.com/prometheus/client_golang/prometheus"
)

type prometheusCounter struct {
	provider       *Prometheus
	promCounterVec *prometheus.CounterVec
}

func (counter *prometheusCounter) Inc(labelValues ...string) {
	labelValues = append(labelValues, counter.provider.label)
	counter.promCounterVec.WithLabelValues(labelValues...).Inc()
}

func (counter *prometheusCounter) Add(value float64, labelValues ...string) {
	labelValues = append(labelValues, counter.provider.label)
	counter.promCounterVec.WithLabelValues(labelValues...).Add(value)
}

type prometheusGauge struct {
	provider     *Prometheus
	promGaugeVec *prometheus.GaugeVec
}

func (gauge *prometheusGauge) Set(value float64, labelValues ...string) {
	labelValues = append(labelValues, gauge.provider.label)
	gauge.promGaugeVec.WithLabelValues(labelValues...).Set(value)
}

type prometheusSummary struct {
	provider       *Prometheus
	promSummaryVec *prometheus.SummaryVec
}

func (summary *prometheusSummary) Observe(value float64, labelValues ...string) {
	labelValues = append(labelValues, summary.provider.label)
	summary.promSummaryVec.WithLabelValues(labelValues...).Observe(value)
}

// Prometheus is an implementation of MetricsProvider that uses Prometheus.
// See https://github.com/prometheus/client_golang
type Prometheus struct {
	label     string
	Registry  *prometheus.Registry
	summaries map[string]*prometheus.SummaryVec
	counters  map[string]*prometheus.CounterVec
	gauges    map[string]*prometheus.GaugeVec
}

// NewPrometheus creates new Prometheus instance.
func NewPrometheus(label string) *Prometheus {
	return &Prometheus{
		label,
		prometheus.NewRegistry(),
		make(map[string]*prometheus.SummaryVec),
		make(map[string]*prometheus.CounterVec),
		make(map[string]*prometheus.GaugeVec),
	}
}

// NewCounter creates a new prometheus CounterVec
func (provider *Prometheus) NewCounter(name string, help string, labelNames ...string) Counter {
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

// NewGauge creates a new prometheus GaugeVec
func (provider *Prometheus) NewGauge(name string, help string, labelNames ...string) Gauge {
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

// NewSummary creates a new prometheus SummaryVec
func (provider *Prometheus) NewSummary(name string, help string, labelNames ...string) Summary {
	labelNames = append(labelNames, "label")
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
