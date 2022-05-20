package stream

import (
	"sync"
	"testing"

	"github.com/tufitko/kafkamirror/pkg/logging"
	"github.com/tufitko/kafkamirror/pkg/stream/mocks"
	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestProducer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topic := "test-topic"
	topicRetention := "3600000"
	metadata := []*sarama.TopicMetadata{
		{
			Err:  sarama.ErrUnknownTopicOrPartition,
			Name: topic,
		},
	}
	sentMessages := make([]interface{}, 0)
	wg := new(sync.WaitGroup)

	initProducerMetrics("test")

	sp := mocks.NewMockSyncProducer(ctrl)
	sp.EXPECT().Close().Times(1)
	sp.EXPECT().SendMessage(gomock.Any()).Do(func(msg *sarama.ProducerMessage) {
		defer wg.Done()
		sentMessages = append(sentMessages, msg.Key)
	}).MinTimes(1)

	ca := mocks.NewMockClusterAdmin(ctrl)
	ca.EXPECT().Close().Times(1)
	ca.EXPECT().DescribeTopics(gomock.Any()).DoAndReturn(func(topics []string) ([]*sarama.TopicMetadata, error) {
		return metadata, nil
	}).MinTimes(3).MaxTimes(3)
	ca.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), gomock.Any()).MinTimes(1).MaxTimes(1)
	ca.EXPECT().DescribeConfig(gomock.Any()).DoAndReturn(func(cr sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
		// 2 hour retention
		return []sarama.ConfigEntry{
			{
				Name:  topic,
				Value: "3600000",
			},
		}, nil
	}).MaxTimes(2)
	ca.EXPECT().AlterConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MinTimes(1).MaxTimes(1)

	p := Producer{
		topic:             topic,
		kafkaProducer:     sp,
		kafkaClusterAdmin: ca,
		logger:            logging.NilLogger,
		retryChan:         make(chan *sarama.ProducerMessage, 1),
	}

	assert.Nil(t, p.AdjustTopic(1, 3, &topicRetention))
	metadata[0].Err = sarama.ErrNoError
	assert.Nil(t, p.AdjustTopic(1, 3, &topicRetention))
	topicRetention = "1000000"
	assert.Nil(t, p.AdjustTopic(1, 3, &topicRetention))

	p.Start()
	messageKeys := []string{"0", "1", "3", "4"}
	for _, msgKey := range messageKeys {
		wg.Add(1)

		err := p.Send(msgKey, "test", nil)
		assert.NoError(t, err)
	}
	wg.Wait()
	p.Stop()

	if assert.Len(t, sentMessages, len(messageKeys)) {
		for i, key := range messageKeys {
			assert.EqualValues(t, key, sentMessages[i])
		}
	}
}
