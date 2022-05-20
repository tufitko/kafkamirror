package stream

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/tufitko/kafkamirror/pkg/logging"
	"github.com/tufitko/kafkamirror/pkg/retry"
)

const (
	OffsetNewest = sarama.OffsetNewest
	OffsetOldest = sarama.OffsetOldest
)

type ConsumerGroupSession interface {
	MarkOffset(topic string, partition int32, offset int64, metadata string)
	ResetOffset(topic string, partition int32, offset int64, metadata string)
	GenerationID() int32
}

type Consumer struct {
	sync.RWMutex
	consumerGroupHandler sarama.ConsumerGroup
	offsetManager        sarama.OffsetManager
	session              sarama.ConsumerGroupSession
	broker               string
	groupID              string
	topics               []string
	config               *ConsumerConfig
	wg                   sync.WaitGroup
	logger               logging.Logger
	handler              func(*Message) error
	errorHandler         func(err error)
}

type ConsumerConfig struct {
	Logger   logging.Logger
	ClientID string

	Connect struct {
		RetryCount int
		RetryDelay time.Duration
		RetryRate  float64
	}

	Consume struct {
		RetryCount        int
		RetryDelay        time.Duration
		RetryRate         float64
		MaxProcessingTime time.Duration
	}

	BufferSize        int
	Autocommit        bool
	InitialOffset     int64
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	ReadTimeout       time.Duration
	TryReconnect      bool
}

func NewConsumerConfig() *ConsumerConfig {
	c := &ConsumerConfig{}

	c.Connect.RetryCount = 5
	c.Connect.RetryDelay = 5 * time.Second
	c.Connect.RetryRate = 2

	c.Consume.RetryCount = 1
	c.Consume.RetryDelay = 250 * time.Millisecond
	c.Consume.RetryRate = 1

	c.BufferSize = 10
	c.InitialOffset = sarama.OffsetOldest

	return c
}

func NewConsumer(
	broker string,
	groupID string,
	config *ConsumerConfig,
	topics ...string,
) *Consumer {
	return &Consumer{
		broker:  broker,
		groupID: groupID,
		topics:  topics,
		config:  config,
		logger:  config.Logger.WithField("service", "consumer"),
	}
}

func (c *Consumer) Start() {
	c.connect()
}

func (c *Consumer) Stop() {
	if err := c.consumerGroupHandler.Close(); err != nil {
		c.logger.WithError(err).Error("consumer group handler close error")
	}
	if err := c.offsetManager.Close(); err != nil {
		c.logger.WithError(err).Error("consumer offset manager close error")
	}

	c.wg.Wait()
}

func (c *Consumer) GetOffsetMetadata(topic string, partition int32) (string, error) {
	var err error
	var pom sarama.PartitionOffsetManager
	pom, err = c.offsetManager.ManagePartition(topic, partition)
	if err != nil {
		c.logger.WithError(err).Error("manage partition error")
		return "", err
	}
	_, metadata := pom.NextOffset()
	return metadata, nil
}

func (c *Consumer) GenerationID() int32 {
	c.RLock()
	defer c.RUnlock()
	return c.session.GenerationID()
}

// PartitionsClaims returns assigned to consumer partitions by topic
func (c *Consumer) PartitionsClaims() map[string][]int32 {
	if c.session == nil {
		return make(map[string][]int32)
	}
	return c.session.Claims()
}

// Marks the provided offset.
//
// Note: calling MarkOffset does not necessarily commit the offset to the backend
// store immediately for efficiency reasons, and it may never be committed if
// your application crashes. This means that you may end up processing the same
// message twice, and your processing should ideally be idempotent.
func (c *Consumer) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	// To follow upstream conventions, you are expected to mark the offset of the
	// next message to read, not the last message read. Thus, when calling `MarkOffset`
	// you should typically add one to the offset of the last consumed message.
	c.session.MarkOffset(topic, partition, offset+1, metadata)
}

// ResetOffset resets to the provided offset, alongside a metadata string that
// represents the state of the partition consumer at that point in time. Reset
// acts as a counterpart to MarkOffset, the difference being that it allows to
// reset an offset to an earlier or smaller value, where MarkOffset only
// allows incrementing the offset. cf MarkOffset for more details.
func (c *Consumer) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	c.session.ResetOffset(topic, partition, offset, metadata)
}

func (c *Consumer) HandleFunc(fn func(*Message) error) {
	c.handler = fn
}

func (c *Consumer) ErrorHandlerFunc(fn func(err error)) {
	c.errorHandler = fn
}

func (c *Consumer) connect() {
	// Try to close previous connections.
	if c.consumerGroupHandler != nil {
		_ = c.consumerGroupHandler.Close() // nolint
	}
	if c.offsetManager != nil {
		_ = c.offsetManager.Close() // nolint
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_5_0_0
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.ClientID = c.config.ClientID
	saramaConfig.ChannelBufferSize = c.config.BufferSize
	saramaConfig.Consumer.Offsets.Initial = c.config.InitialOffset

	if c.config.SessionTimeout > 0 {
		saramaConfig.Consumer.Group.Session.Timeout = c.config.SessionTimeout
	}
	if c.config.HeartbeatInterval > 0 {
		saramaConfig.Consumer.Group.Heartbeat.Interval = c.config.HeartbeatInterval
	}
	if c.config.ReadTimeout > 0 {
		saramaConfig.Net.ReadTimeout = c.config.ReadTimeout
	}
	if c.config.Consume.MaxProcessingTime > 0 {
		saramaConfig.Consumer.MaxProcessingTime = c.config.Consume.MaxProcessingTime
	}

	var err error
	var saramaClient sarama.Client
	err = retry.
		New(c.config.Connect.RetryCount, c.config.Connect.RetryDelay, c.config.Connect.RetryRate).
		Do(func(attempt int) error {
			c.logger.
				WithField("attempt", attempt).
				Info("connecting to kafka")
			saramaClient, err = sarama.NewClient(strings.Split(c.broker, ","), saramaConfig)
			if err != nil {
				c.logger.WithError(err).Error("connection to kafka failed")
			}
			return err
		})
	if err != nil {
		c.logger.WithError(err).Error("connection to kafka failed")
		if c.errorHandler != nil {
			c.errorHandler(err)
		}
		return
	}

	var consumerGroupHandler sarama.ConsumerGroup
	consumerGroupHandler, err = sarama.NewConsumerGroupFromClient(c.groupID, saramaClient)
	if err != nil {
		c.logger.WithError(err).Error("consumer group handler constructor failed")
		if c.errorHandler != nil {
			c.errorHandler(err)
		}
		return
	}

	var offsetManager sarama.OffsetManager
	offsetManager, err = sarama.NewOffsetManagerFromClient(c.groupID, saramaClient)
	if err != nil {
		c.logger.WithError(err).Error("offset manager constructor failed")
		if c.errorHandler != nil {
			c.errorHandler(err)
		}
		return
	}

	c.logger.Info("connected to kafka")

	c.Lock()
	defer c.Unlock()
	c.consumerGroupHandler = consumerGroupHandler
	c.offsetManager = offsetManager
	c.wg.Add(2)
	go c.consume()
	go c.errors()
}

func (c *Consumer) consume() {
	defer c.wg.Done()

	if c.handler == nil {
		c.logger.Error("handler function is not registered")
		return
	}

	consumer := consumerGroupDispatcher{
		Consumer: c,
	}

	// The loop is used because consuming should be restarted in case of rebalanced consumer group.
	for {
		err := retry.
			New(c.config.Consume.RetryCount, c.config.Consume.RetryDelay, c.config.Consume.RetryRate).
			Do(func(attempt int) error {
				c.logger.WithField("attempt", attempt).Info("begin consume")

				err := c.consumerGroupHandler.Consume(context.Background(), c.topics, &consumer)

				if err == sarama.ErrClosedConsumerGroup {
					c.logger.Info("consumer group is stopped")
					return &retry.ErrStopRetry{}
				}
				return err
			})

		if _, ok := err.(*retry.ErrStopRetry); ok {
			break
		}

		if err != nil {
			if c.config.TryReconnect {
				c.logger.WithError(err).Warn("trying to reconnect to kafka")
				go c.connect()
				break
			}

			c.logger.WithError(err).Error("consume error")
			if c.errorHandler != nil {
				c.errorHandler(err)
			}
			break
		}
	}
}

func (c *Consumer) shouldCommit() bool {
	c.RLock()
	defer c.RUnlock()
	return c.config.Autocommit
}

func (c *Consumer) errors() {
	defer c.wg.Done()

	for err := range c.consumerGroupHandler.Errors() {
		if err, ok := err.(*sarama.ConsumerError); ok {
			switch err.Unwrap() {
			// just warn about some retriable errors https://kafka.apache.org/protocol#protocol_error_codes
			case sarama.ErrUnknownMemberId,
				sarama.ErrRequestTimedOut:
				c.logger.WithError(err).Warn("consumer client error")
				continue
			}
		}
		c.logger.WithError(err).Error("consumer client error")
	}
}
