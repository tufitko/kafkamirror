package main

import (
	"flag"
	"fmt"
	"github.com/tufitko/kafkamirror/pkg/labels"
	"github.com/tufitko/kafkamirror/pkg/logging"
	"github.com/tufitko/kafkamirror/pkg/metric"
	"github.com/tufitko/kafkamirror/pkg/service"
	"github.com/tufitko/kafkamirror/pkg/stream"
	"strings"
	"time"
)

const appName = "kafkamirror"

var (
	diagnosticAddr            = flag.String("diagnostic-addr", ":7070", "Address for gathering metrics and pprof")
	connectRetryCount         = flag.Int("connect-retry-count", 3, "Number of max attempts for event stream connection")
	connectRetryDelay         = flag.Duration("connect-retry-delay", time.Second*5, "Delay between event stream reconnects")
	consumerBufferSize        = flag.Int("consumer-buffer-size", 10, "Max number of events that can be queued in consumer before it blocks")
	consumerMaxProcessingTime = flag.Duration("consumer-max-process-time", 300*time.Millisecond, "Message max processing time")
	topics                    = flag.String("topics", "", "Topics to mirror")
	sourceBroker              = flag.String("source-broker", "broker:9092", "Kafka source broker")
	destinationBroker         = flag.String("destination-broker", "broker:9092", "Kafka source broker")
	consumerGroup             = flag.String("consumer-group", appName, "Name of consumer group")
	producerRetryInterval     = flag.Duration("producer-retry-interval", time.Second, "Producer retry interval")
	producerPoolBuffSize      = flag.Int("producer-buff-size", 256, "Producer pool buffer size")
	producerRetryBuffSize     = flag.Int("producer-retry-buff-size", 100, "Producer retry buffer size")
	producerFlushBytes        = flag.Int("producer-flush-bytes", 0, "Producer flush bytes")
	producerFlushMessages     = flag.Int("producer-flush-messages", 0, "Producer flush messages")
	producerFlushFrequency    = flag.Duration("producer-flush-frequency", 0, "Producer flush frequency")
	producerFlushMaxMessages  = flag.Int("producer-flush-max-messages", 0, "Producer flush max messages")
	producerCompression       = flag.String("producer-compression", "none", "Producer compression codec")
	producerCompressionLevel  = flag.Int("producer-compression-level", 1, "Producer compression level")
	skipErrors                = flag.Bool("skip-errors", false, "Skip errors while mirroring message")

	metrics metric.DefaultWorkerMetrics
)

func initMetrics() {
	metrics = metric.NewDefaultWorkerMetrics("worker", []float64{.25, .5, 1, 2.5, 5, 10, 15, 20, 25})
	metrics.MustRegister()
}

func main() {
	flag.Parse()
	labels.Add(map[string]string{"app": appName})
	initMetrics()
	service.SetInfo(labels.Labels)
	logging.SetDefaultFields(labels.GetLoggerLabels())

	logging.Info("starting app")
	service.StartDiagnosticsServer(*diagnosticAddr)

	consumerConfig := stream.NewConsumerConfig()
	consumerConfig.ClientID = appName
	consumerConfig.Logger = logging.DefaultLogger
	consumerConfig.Connect.RetryCount = *connectRetryCount
	consumerConfig.Connect.RetryDelay = *connectRetryDelay
	consumerConfig.BufferSize = *consumerBufferSize
	consumerConfig.InitialOffset = stream.OffsetNewest
	consumerConfig.Consume.MaxProcessingTime = *consumerMaxProcessingTime

	parsedConsumeTopics := strings.Split(*topics, ",")
	consumer := stream.NewConsumer(*sourceBroker, *consumerGroup, consumerConfig, parsedConsumeTopics...)

	producers := make(map[string]*stream.Producer)
	for _, topic := range parsedConsumeTopics {
		sender, err := stream.NewProducer(
			appName,
			*destinationBroker,
			topic,
			*connectRetryCount,
			*connectRetryDelay,
			*producerPoolBuffSize,
			*producerRetryBuffSize,
			*producerRetryInterval,
			logging.DefaultLogger,
			stream.Flush(*producerFlushBytes, *producerFlushMessages, *producerFlushFrequency, *producerFlushMaxMessages),
			stream.Compress(*producerCompression, *producerCompressionLevel),
		)
		if err != nil {
			logging.WithError(err).Fatal("failed to create events topic producer")
		}
		producers[topic] = sender
	}

	handler := func(message *stream.Message) error {
		producer, ok := producers[message.Topic]
		if !ok {
			return fmt.Errorf("not found producer for topic '%s'", message.Topic)
		}

		return producer.Send(message.Key, message.Type, message.Data, message.Meta)
	}

	consumer.HandleFunc(func(message *stream.Message) error {
		// Calculate duration for metrics.
		defer func(startTime time.Time) {
			metrics.HandlerDurationSeconds.WithLabelValues(message.Type).Observe(time.Since(startTime).Seconds())
		}(time.Now())

		if err := handler(message); err != nil {
			logging.WithFields(logging.Fields{
				"key":            message.Key,
				"message_type":   message.Type,
				"topic":          message.Topic,
				"content_length": len(message.Data),
				"partition":      message.Partition,
				"offset":         message.Offset,
			}).Errorf("handle error: %w", err)
			metrics.SkippedMessagesCount.WithLabelValues(message.Type).Inc()
			if !*skipErrors {
				return err
			}
		} else {
			metrics.DeliveredMessagesCount.WithLabelValues(message.Type).Inc()
		}
		consumer.MarkOffset(message.Topic, message.Partition, message.Offset, "")
		return nil
	})

	ss := startStopper{
		start: func() {
			consumer.Start()
			for _, p := range producers {
				p.Start()
			}
		},
		stop: func() {
			consumer.Stop()
			for _, p := range producers {
				p.Stop()
			}
		},
	}

	consumer.ErrorHandlerFunc(func(err error) {
		logging.WithError(err).Info("stopping application")
		service.SetReady(false)
		ss.Stop()
	})

	service.Run(ss)
}

type startStopper struct {
	start func()
	stop  func()
}

func (s startStopper) Start() {
	s.start()
}

func (s startStopper) Stop() {
	s.stop()
}
