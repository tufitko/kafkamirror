package stream

import (
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

const (
	messageTypeKey = "type"
)

const (
	metaPrefix                 = "meta_"
	MetaKeyStartSearchUnixTime = "start_search_unixtime"
	MetaKeyStreamSendUnixTime  = "stream_send_unixtime"
	MetaKeyRequestId           = "request_id"
	MetaGateName               = "gate_name"
	MetaSearchKey              = "search_key"
	MetaAgentId                = "agent_id"
	MetaSearchId               = "search_id"
	MetaPartIndex              = "part_index"
	MetaTTL                    = "ttl"
	MetaTracing                = "tracing"
)

func GetMessageType(headers []*sarama.RecordHeader) string {
	var msgType string
	for _, h := range headers {
		if string(h.Key) == messageTypeKey {
			msgType = string(h.Value)
			break
		}
	}
	return msgType
}

// GetMessageMeta getsd metadata from headers
func GetMessageMeta(headers []*sarama.RecordHeader) map[string]string {
	meta := make(map[string]string)

	for _, h := range headers {
		if strings.HasPrefix(string(h.Key), metaPrefix) {
			meta[strings.TrimPrefix(string(h.Key), metaPrefix)] = string(h.Value)
		}
	}

	return meta
}

func MetaStreamSendTimestamp() sarama.RecordHeader {
	return sarama.RecordHeader{
		Key:   []byte(metaPrefix + MetaKeyStreamSendUnixTime),
		Value: []byte(time.Now().Format(time.RFC3339Nano)),
	}
}

// MetaToHeaders transforms metadata to sarama headers
func MetaToHeaders(meta map[string]string) []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, 0, len(meta))
	for k, v := range meta {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(metaPrefix + k),
			Value: []byte(v),
		})
	}

	return headers
}

type Message struct {
	Key       string
	Type      string
	Data      []byte
	Topic     string
	Partition int32
	Offset    int64
	Meta      map[string]string
}
