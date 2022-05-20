package heap_test

import (
	"testing"

	"github.com/tufitko/kafkamirror/pkg/heap"
	"github.com/stretchr/testify/assert"
)

func TestHeap(t *testing.T) {
	h := heap.NewHeap()
	h.Push(5)
	h.Push(2)
	h.Push(3)
	assert.Equal(t, int64(2), h.Peek())
	h.Push(1)
	assert.Equal(t, int64(1), h.Peek())
	h.Remove(3)
	h.Remove(1)
	assert.Equal(t, int64(2), h.Peek())
	h.Push(10)
	h.Push(8)
	h.Remove(2)
	assert.Equal(t, int64(5), h.Peek())
	h.Remove(5)
	assert.Equal(t, int64(8), h.Peek())
	h.Remove(10)
	h.Remove(8)
	assert.Equal(t, int64(-1), h.Peek())
}
