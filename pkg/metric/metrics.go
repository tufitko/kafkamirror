package metric

import (
	"github.com/tufitko/kafkamirror/pkg/labels"
	"github.com/prometheus/client_golang/prometheus"
)

type DefaultWorkerMetrics struct {
	ConsumedMessagesCount  *prometheus.CounterVec
	ConsumeErrorsCount     *prometheus.CounterVec
	DeliveredMessagesCount *prometheus.CounterVec
	DeliveryErrorsCount    *prometheus.CounterVec
	SkippedMessagesCount   *prometheus.CounterVec
	HandlerDurationSeconds *prometheus.HistogramVec
}

type HandlerDurationBuckets []float64

func NewDefaultWorkerMetrics(namespace string, handlerDurationBuckets HandlerDurationBuckets) DefaultWorkerMetrics {
	return DefaultWorkerMetrics{
		ConsumedMessagesCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Name:        "message_read",
			Help:        "Count of consumed messages of exact type",
			ConstLabels: labels.Labels,
		}, []string{"message_type"}),
		ConsumeErrorsCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Name:        "message_read_error",
			Help:        "Count of consumer errors of exact type",
			ConstLabels: labels.Labels,
		}, []string{"message_type"}),
		DeliveredMessagesCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Name:        "message_deliver",
			Help:        "Count of delivered messages of exact type",
			ConstLabels: labels.Labels,
		}, []string{"message_type"}),
		DeliveryErrorsCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "message_deliver_error",
			Help:      "Count of delivery errors of exact type",
		}, []string{"message_type"}),
		SkippedMessagesCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Name:        "message_skip",
			Help:        "Count of skipped messages of exact type",
			ConstLabels: labels.Labels,
		}, []string{"message_type"}),
		HandlerDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Name:        "handler_duration_seconds",
			Help:        "Handler duration histogram",
			ConstLabels: labels.Labels,
			Buckets:     handlerDurationBuckets,
		}, []string{"message_type"}),
	}
}

func (m *DefaultWorkerMetrics) MustRegister() {
	prometheus.MustRegister(m.ConsumedMessagesCount)
	prometheus.MustRegister(m.ConsumeErrorsCount)
	prometheus.MustRegister(m.DeliveredMessagesCount)
	prometheus.MustRegister(m.DeliveryErrorsCount)
	prometheus.MustRegister(m.SkippedMessagesCount)
	prometheus.MustRegister(m.HandlerDurationSeconds)
}
