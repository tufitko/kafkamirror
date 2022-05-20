package stream

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/tufitko/kafkamirror/pkg/encoding/json"
	"github.com/tufitko/kafkamirror/pkg/logging"
	"github.com/tufitko/kafkamirror/pkg/retry"
)

const (
	defaultCompressionLevel   = 1
	producerFailedMessagesDir = `/tmp/producer_failed_messages`
	retentionConfigEntry      = "retention.ms"
)

func init() {
	sarama.MaxRequestSize = 160 * 1024 * 1024
	sarama.MaxResponseSize = 160 * 1024 * 1024
}

// Producer wraps kafka writer
type Producer struct {
	kafkaProducer     sarama.SyncProducer
	kafkaClusterAdmin sarama.ClusterAdmin
	topic             string
	logger            logging.Logger
	closed            bool
	failedMessagesDir string
	fallbackReadyFunc func(ready bool)
	retryChan         chan *sarama.ProducerMessage
	retryDelay        time.Duration
	wg                sync.WaitGroup
	lock              sync.RWMutex
}

type ProducerOp func(*sarama.Config)

func Flush(bytes int, messages int, freq time.Duration, maxMessages int) ProducerOp {
	return func(config *sarama.Config) {
		config.Producer.Flush.Bytes = bytes
		config.Producer.Flush.Messages = messages
		config.Producer.Flush.Frequency = freq
		config.Producer.Flush.MaxMessages = maxMessages
	}
}

func Compress(codec string, level int) ProducerOp {
	var c sarama.CompressionCodec = -1
	for i := 0; i < 5; i++ {
		if sarama.CompressionCodec(i).String() == codec {
			c = sarama.CompressionCodec(i)
			break
		}
	}

	if c == -1 {
		return func(config *sarama.Config) {
			// not found codec
		}
	}

	return func(config *sarama.Config) {
		config.Producer.Compression = c
		config.Producer.CompressionLevel = level
	}
}

// NewProducer common constructor func
func NewProducer(
	appName string,
	connStr string,
	topic string,
	connectMaxRetryCount int,
	connectRetryDelay time.Duration,
	bufferSize int,
	retryBufferSize int,
	retryDelay time.Duration,
	logger logging.Logger,
	opts ...ProducerOp,
) (*Producer, error) {
	logger = logger.WithField("service", "producer")

	onceInitProducerMetrics.Do(func() {
		initProducerMetrics(appName)
	})

	// setup proper config
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.MaxMessageBytes = 1024 * 1024 * 150
	config.ChannelBufferSize = bufferSize
	config.Version = sarama.V2_5_0_0
	config.ClientID = "delta"
	config.Producer.Compression = sarama.CompressionZSTD
	config.Producer.CompressionLevel = defaultCompressionLevel
	for _, op := range opts {
		op(config)
	}

	var kafkaProducer sarama.SyncProducer
	var err error
	err = retry.New(connectMaxRetryCount, connectRetryDelay, 2).Do(func(attempt int) error {
		logger.WithField("attempt", attempt).Info("connecting to kafka")
		kafkaProducer, err = sarama.NewSyncProducer(strings.Split(connStr, ","), config)
		if err != nil {
			logger.WithError(err).Error("connection to kafka failed")
		}
		return err
	})
	if err != nil {
		return nil, errors.Wrapf(err, "connection to kafka failed after %d attempts", connectMaxRetryCount)
	}

	var kafkaClusterAdmin sarama.ClusterAdmin
	err = retry.New(connectMaxRetryCount, connectRetryDelay, 2).Do(func(attempt int) error {
		logger.WithField("attempt", attempt).Info("connecting to kafka")
		kafkaClusterAdmin, err = sarama.NewClusterAdmin(strings.Split(connStr, ","), config)
		if err != nil {
			logger.WithError(err).Error("connection to kafka failed")
		}
		return err
	})
	if err != nil {
		return nil, errors.Wrapf(err, "connection to kafka failed after %d attempts", connectMaxRetryCount)
	}

	return &Producer{
		kafkaProducer:     kafkaProducer,
		kafkaClusterAdmin: kafkaClusterAdmin,
		topic:             topic,
		retryChan:         make(chan *sarama.ProducerMessage, retryBufferSize),
		failedMessagesDir: producerFailedMessagesDir,
		logger:            logger,
		retryDelay:        retryDelay,
	}, nil
}

// SetFailedMessagesDir overwrites failedMessagesDir
func (p *Producer) SetFailedMessagesDir(path string) {
	p.failedMessagesDir = path
}

// Send wraps message into stream message with specified type and performs send
// Also, metadata can be sent. It is not required to unmarshal message to get access to meta.
// First key parameter is used for determining partition by hash.
func (p *Producer) Send(key string, mtype string, data []byte, meta ...map[string]string) error {
	producerMessageSize.WithLabelValues(mtype, p.topic).Observe(float64(len(data)))
	start := time.Now()
	defer func() {
		producerMessageDurationSeconds.WithLabelValues(mtype, p.topic).Observe(time.Since(start).Seconds())
	}()
	return p.send(sarama.StringEncoder(key), mtype, data, meta...)
}

// SendRandom behaves as Send, but chooses random partition.
func (p *Producer) SendRandom(mtype string, data []byte, meta ...map[string]string) error {
	producerMessageSize.WithLabelValues(mtype, p.topic).Observe(float64(len(data)))
	start := time.Now()
	defer func() {
		producerMessageDurationSeconds.WithLabelValues(mtype, p.topic).Observe(time.Since(start).Seconds())
	}()
	// Note what sarama hashPartitioner will use randomPartitioner if key is nil.
	return p.send(nil, mtype, data, meta...)
}

// SendWithoutRetrying as Send, without retrying mechanism
func (p *Producer) SendWithoutRetrying(key string, mtype string, data []byte, meta ...map[string]string) error {
	producerMessageSize.WithLabelValues(mtype, p.topic).Observe(float64(len(data)))
	start := time.Now()
	defer func() {
		producerMessageDurationSeconds.WithLabelValues(mtype, p.topic).Observe(time.Since(start).Seconds())
	}()
	return p.sendWithoutRetrying(sarama.StringEncoder(key), mtype, data, meta...)
}

func (p *Producer) send(key sarama.Encoder, mtype string, data []byte, meta ...map[string]string) error {
	msgCtxLogger := p.logger.WithFields(logging.Fields{
		"key":            key,
		"message_type":   mtype,
		"content_length": len(data),
	})
	msgCtxLogger.Debug("sending message")

	msg := p.buildMessage(mtype, meta, key, data)

	_, _, err := p.kafkaProducer.SendMessage(&msg)

	if err != nil {
		// set delta ready status unavailable
		if p.fallbackReadyFunc != nil {
			p.fallbackReadyFunc(false)
		}

		if p.isClosed() {
			msgCtxLogger.WithError(err).Error("send error on closed producer")
			if fileErr := p.writeFile(&msg); fileErr != nil {
				p.logger.WithError(fileErr).Error("error writing files to disk")
			}
		} else {
			p.retryChan <- &msg
		}

		return errors.Wrap(err, "send message error")
	}

	msgCtxLogger.Debug("message sent successfully")
	return nil
}

func (p *Producer) sendWithoutRetrying(key sarama.Encoder, mtype string, data []byte, meta ...map[string]string) error {
	msgCtxLogger := p.logger.WithFields(logging.Fields{
		"key":            key,
		"message_type":   mtype,
		"content_length": len(data),
	})
	msgCtxLogger.Debug("sending message")

	msg := p.buildMessage(mtype, meta, key, data)

	_, _, err := p.kafkaProducer.SendMessage(&msg)
	if err != nil {
		msgCtxLogger.WithError(err).Error("send error on closed producer")
	} else {
		msgCtxLogger.Debug("message sent successfully")
	}

	return errors.Wrap(err, "send message error")
}

func (p *Producer) buildMessage(mtype string, meta []map[string]string, key sarama.Encoder, data []byte) sarama.ProducerMessage {
	headers := []sarama.RecordHeader{{
		Key:   []byte(messageTypeKey),
		Value: []byte(mtype),
	}}
	headers = append(headers, MetaStreamSendTimestamp())

	if len(meta) == 1 {
		headers = append(headers, MetaToHeaders(meta[0])...)
	}

	msg := sarama.ProducerMessage{
		Topic:    p.topic,
		Key:      key,
		Value:    sarama.ByteEncoder(data),
		Headers:  headers,
		Metadata: mtype,
	}
	return msg
}

// AdjustTopic created topic if not exists or checks topic's params
func (p *Producer) AdjustTopic(numPartitions int, replicaFactor int, retention *string) error {
	if retention == nil {
		return errors.New("invalid nil retention")
	}
	logger := p.logger.WithField("topic", p.topic)
	meta, err := p.kafkaClusterAdmin.DescribeTopics([]string{p.topic})
	if err != nil {
		logger.WithError(err).Error("cannot get topic information")
		return err
	}
	if len(meta) != 1 {
		return errors.New("invalid topic metadata")
	}
	entries := map[string]*string{retentionConfigEntry: retention}
	switch meta[0].Err {
	case sarama.ErrUnknownTopicOrPartition:
		// Need to create topic
		logger.Info("creating topic")
		if err = p.kafkaClusterAdmin.CreateTopic(p.topic, &sarama.TopicDetail{
			NumPartitions:     int32(numPartitions),
			ReplicationFactor: int16(replicaFactor),
			ConfigEntries:     entries,
		}, false); err != nil {
			logger.WithError(err).Error("cannot create topic")
			return err
		}
		logger.Info("topic has been created successfully")
	case sarama.ErrNoError:
		// Need to update topic retention if need
		var configEntries []sarama.ConfigEntry
		configEntries, err = p.kafkaClusterAdmin.DescribeConfig(sarama.ConfigResource{
			Type:        sarama.TopicResource,
			Name:        p.topic,
			ConfigNames: []string{retentionConfigEntry},
		})
		if err != nil {
			logger.WithError(err).Error("cannot describe topic config")
			return err
		}
		if len(configEntries) != 1 {
			return errors.New("invalid number of config entries")
		}

		if configEntries[0].Value == *retention {
			logger.Info("topic retention is the same. no need to set it again")
			return nil
		}

		logger.WithField("retention_milliseconds", *retention).Info("setting retention to retention_milliseconds")
		if err = p.kafkaClusterAdmin.AlterConfig(sarama.TopicResource, p.topic, entries, false); err != nil {
			logger.WithError(err).WithField("retention_milliseconds", *retention).Error("cannot set retention to retention_milliseconds")
			return err
		}
		logger.Info("retention has been set successfully")
	default:
		logger.WithError(meta[0].Err).Error("unexpected topic metadata error")
		return err
	}

	return nil
}

// Start starts the retry loop routine
func (p *Producer) Start() {
	p.wg.Add(1)
	go p.retrySend()
}

// Stop stops the retry routine and kafka producer
func (p *Producer) Stop() {
	p.lock.Lock()
	close(p.retryChan)
	p.closed = true
	p.lock.Unlock()

	p.wg.Wait()
	if err := p.kafkaProducer.Close(); err != nil {
		p.logger.WithError(err).Error("close producer error")
	}
	if err := p.kafkaClusterAdmin.Close(); err != nil {
		p.logger.WithError(err).Error("close cluster admin error")
	}
}

// SetFallbackReadyFunc is a callback setter for receiving ready state changes
func (p *Producer) SetFallbackReadyFunc(fallbackReadyFunc func(ready bool)) {
	p.fallbackReadyFunc = fallbackReadyFunc
}

// Ping return nil of ping successful or error
func (p *Producer) Ping() error {
	_, err := p.kafkaClusterAdmin.ListTopics()
	return err
}

func (p *Producer) isClosed() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.closed
}

func (p *Producer) retrySend() {
	defer p.wg.Done()
	for msg := range p.retryChan {
		log := p.logger.WithFields(logging.Fields{
			"key":            msg.Key,
			"message_type":   msg.Metadata,
			"content_length": msg.Value.Length(),
		})

		for {
			log.Info("retry sending message")
			if _, _, err := p.kafkaProducer.SendMessage(msg); err != nil {
				log.WithError(err).Error("send message error on retry")

				if p.isClosed() {
					log.Info("dump message to disk")
					if fileErr := p.writeFile(msg); fileErr != nil {
						log.WithError(fileErr).Error("error writing files to disk")
					}
					break
				}

				time.Sleep(p.retryDelay)
				continue
			}

			log.Info("message sent successfully after retry")
			break
		}

		if len(p.retryChan) == 0 && p.fallbackReadyFunc != nil && !p.isClosed() {
			p.fallbackReadyFunc(true)
		}
	}
}

// writeFile writes message to file into failedMessagesDir
func (p *Producer) writeFile(msg *sarama.ProducerMessage) (err error) {
	// check that dir exists and create if not
	if err = os.MkdirAll(p.failedMessagesDir, os.ModePerm); err != nil {
		return errors.Wrap(err, "writeFile: create dir "+p.failedMessagesDir+" failed")
	}

	// prepare data for writing
	data, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "writeFile: marshal json error")
	}

	// e.g. type_18600715-ece9-4632-ac03-829178e49799_2018-11-19T19:38:40+07:00
	fileName := fmt.Sprintf("%s/%s_%s_%s", p.failedMessagesDir, msg.Metadata, msg.Key, time.Now().Format(time.RFC3339))

	err = ioutil.WriteFile(fileName, data, 0644)
	return errors.Wrap(err, "writeFile: write file error")
}
