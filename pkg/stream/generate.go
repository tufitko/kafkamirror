//go:generate mockgen -destination=mocks/stream.go -package=mocks "github.com/Shopify/sarama" SyncProducer,ConsumerGroupSession,ClusterAdmin

package stream
