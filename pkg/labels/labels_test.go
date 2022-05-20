package labels_test

import (
	"testing"

	"github.com/tufitko/kafkamirror/pkg/labels"
	"github.com/stretchr/testify/assert"
)

func TestAdd(t *testing.T) {
	assert.Len(t, labels.Labels, 4)
	labels.Add(map[string]string{"foo": "bar"})
	assert.Len(t, labels.Labels, 5)
	assert.Equal(t, "bar", labels.Labels["foo"])
}
