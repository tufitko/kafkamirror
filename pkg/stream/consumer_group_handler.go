package stream

import (
	"github.com/Shopify/sarama"
	"github.com/tufitko/kafkamirror/pkg/logging"
)

type consumerGroupDispatcher struct {
	*Consumer
}

func (c *consumerGroupDispatcher) Setup(session sarama.ConsumerGroupSession) error {
	c.logger.WithField("generation_id", session.GenerationID()).Debug("consumer group set new session")
	c.session = session
	return nil
}

func (*consumerGroupDispatcher) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (c *consumerGroupDispatcher) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	c.logger.WithField("partition", claim.Partition()).Debug("consumer group claims new partition")
	for msg := range claim.Messages() {
		var (
			key         = string(msg.Key)
			messageType = GetMessageType(msg.Headers)
			meta        = GetMessageMeta(msg.Headers)
		)

		autocommit := c.shouldCommit()

		c.logger.
			WithFields(logging.Fields{
				"key":            key,
				"offset":         msg.Offset,
				"partition":      msg.Partition,
				"message_type":   messageType,
				"content_length": len(msg.Value),
				"autocommit":     autocommit,
			}).
			Debug("message is going to be handled")

		if autocommit {
			session.MarkMessage(msg, "")
		}

		err := c.handler(&Message{
			Key:       key,
			Type:      messageType,
			Data:      msg.Value,
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Meta:      meta,
		})

		if err != nil {
			return err
		}
	}

	return nil
}
