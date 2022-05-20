package stream

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetMessageMeta(t *testing.T) {
	headers := []*sarama.RecordHeader{
		{
			Key:   []byte("meta_k"),
			Value: []byte("v"),
		},
		{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
	}
	meta := GetMessageMeta(headers)

	require.Len(t, meta, 1)
	assert.Equal(t, "v", meta["k"])
}

func TestMetaToHeaders(t *testing.T) {
	m := map[string]string{
		"k": "v",
	}

	h := MetaToHeaders(m)

	require.Len(t, h, 1)
	assert.Equal(t, "meta_k", string(h[0].Key))
	assert.Equal(t, "v", string(h[0].Value))
}

func TestMetaStreamSendTimestamp(t *testing.T) {
	h := MetaStreamSendTimestamp()
	assert.Equal(t, "meta_stream_send_unixtime", string(h.Key))
	_, err := time.Parse(time.RFC3339Nano, string(h.Value))
	assert.Nil(t, err)
}
