package stream

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartitionManager_SetMeta(t *testing.T) {
	pm := NewPartitionManager(0, 1)
	meta := map[string]struct{}{
		"test1": {},
		"test2": {},
	}
	pm.SetMeta(meta)
	assert.Equal(t, true, pm.ShouldSkipMessage("test1"))
	assert.Equal(t, true, pm.ShouldSkipMessage("test2"))
	assert.Equal(t, false, pm.ShouldSkipMessage("test3"))
}

func TestPartitionManager_StartAndCLoseSearches(t *testing.T) {
	var initialOffset int64 = 0
	t.Run("try to close search with invalid uuid", func(t *testing.T) {
		pm := NewPartitionManager(initialOffset, 1)
		_, _, err := pm.CloseSearch("invalid")
		assert.NotNil(t, err)
	})

	t.Run("should close 3 consecutive searches", func(t *testing.T) {
		var offset1 int64 = 1
		var offset2 int64 = 5
		var offset3 int64 = 10
		pm := NewPartitionManager(initialOffset, 1)
		pm.InitializeSearch("test1", offset1)
		pm.InitializeSearch("test2", offset2)
		pm.InitializeSearch("test3", offset3)
		offset, meta, err := pm.CloseSearch("test1")
		assert.Nil(t, err)
		assert.Equal(t, offset1, offset)
		assert.Equal(t, "", meta)

		offset, meta, err = pm.CloseSearch("test2")
		assert.Nil(t, err)
		assert.Equal(t, offset2, offset)
		assert.Equal(t, "", meta)

		offset, meta, err = pm.CloseSearch("test3")
		assert.Nil(t, err)
		assert.Equal(t, offset3, offset)
		assert.Equal(t, "", meta)
	})

	t.Run("should close 1 embedded search and the root", func(t *testing.T) {
		var offset1 int64 = 1
		var offset2 int64 = 5
		pm := NewPartitionManager(initialOffset, 1)
		pm.InitializeSearch("test1", offset1)
		pm.InitializeSearch("test2", offset2)
		offset, meta, err := pm.CloseSearch("test2")
		assert.Nil(t, err)
		assert.Equal(t, initialOffset, offset)
		assert.Equal(t, "test2", meta)

		offset, meta, err = pm.CloseSearch("test1")
		assert.Nil(t, err)
		assert.Equal(t, offset1, offset)
		assert.Equal(t, "test2", meta)
	})

	t.Run("should close  2 embedded searches, root and last one", func(t *testing.T) {
		var offset1 int64 = 1
		var offset2 int64 = 5
		var offset3 int64 = 10
		var offset4 int64 = 15
		pm := NewPartitionManager(initialOffset, 1)
		pm.InitializeSearch("test1", offset1)
		pm.InitializeSearch("test2", offset2)
		pm.InitializeSearch("test3", offset3)
		pm.InitializeSearch("test4", offset4)
		offset, meta, err := pm.CloseSearch("test2")
		assert.Nil(t, err)
		assert.Equal(t, initialOffset, offset)
		assert.Equal(t, "test2", meta)

		offset, meta, err = pm.CloseSearch("test3")
		assert.Nil(t, err)
		assert.Equal(t, initialOffset, offset)
		parts := strings.Split(meta, ",")
		assert.Equal(t, 2, len(parts))

		offset, meta, err = pm.CloseSearch("test1")
		assert.Nil(t, err)
		assert.Equal(t, offset1, offset)
		parts = strings.Split(meta, ",")
		assert.Equal(t, 2, len(parts))

		offset, meta, err = pm.CloseSearch("test4")
		assert.Nil(t, err)
		assert.Equal(t, offset4, offset)
		assert.Equal(t, "", meta)
	})
}
