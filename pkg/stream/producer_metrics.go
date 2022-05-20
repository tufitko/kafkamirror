package stream

import (
	"sync"

	"github.com/tufitko/kafkamirror/pkg/labels"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	producerMessageSize            *prometheus.HistogramVec
	producerMessageDurationSeconds *prometheus.HistogramVec
	onceInitProducerMetrics        sync.Once
)

func initProducerMetrics(appName string) {
	producerMessageSize = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   appName,
		Name:        "sender_message_size_bytes",
		Help:        "Sender message size histogram",
		ConstLabels: labels.Labels,
		Buckets:     []float64{1000, 5000, 15000, 50000, 75000, 100000, 150000, 200000, 400000},
	}, []string{"message_type", "topic"})
	producerMessageDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   appName,
		Name:        "sender_message_duration_seconds",
		Help:        "Sender message duration histogram",
		ConstLabels: labels.Labels,
		Buckets:     []float64{0.01, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	}, []string{"message_type", "topic"})
	prometheus.MustRegister(producerMessageSize, producerMessageDurationSeconds)
}
