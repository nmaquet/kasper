package kasper

import "github.com/prometheus/client_golang/prometheus"

type prometheusCounter struct {
	promCounterVec *prometheus.CounterVec
}

func (counter *prometheusCounter) Inc(labelValues ...string) {
	counter.promCounterVec.WithLabelValues(labelValues...).Inc()
}

type prometheusGauge struct {
	promGaugeVec *prometheus.GaugeVec
}

func (gauge *prometheusGauge) Set(value float64, labelValues ...string) {
	gauge.promGaugeVec.WithLabelValues(labelValues...).Set(value)
}

type PrometheusMetricsProvider struct {
	Registry *prometheus.Registry
}

func NewPrometheusMetricsProvider() *PrometheusMetricsProvider {
	return &PrometheusMetricsProvider{
		prometheus.NewRegistry(),
	}
}

func (provider *PrometheusMetricsProvider) NewCounter(name string, help string, labelNames ...string) Counter {
	counterVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "kasper",
			Name: name,
			Help: help,
		},
		labelNames,
	)
	provider.Registry.MustRegister(counterVec)
	return &prometheusCounter{
		counterVec,
	}
}

func (provider *PrometheusMetricsProvider) NewGauge(name string, help string, labelNames ...string) Gauge {
	gaugeVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kasper",
			Name: name,
			Help: help,
		},
		labelNames,
	)
	provider.Registry.MustRegister(gaugeVec)
	return &prometheusGauge{
		gaugeVec,
	}
}
