package stream

import (
	"fmt"
	"strings"
	"sync"

	"github.com/tufitko/kafkamirror/pkg/heap"
	"github.com/tufitko/kafkamirror/pkg/logging"
	"github.com/pkg/errors"
)

// Partition manager keeps state of the partition
type PartitionManager struct {
	sync.RWMutex
	offset            int64
	generationID      int32
	heap              *heap.Heap
	inProgress        map[string]int64
	meta              map[string]struct{}
	processedSearches map[string]int64
}

// NewPartitionManager() returns new PartitionManager instance
func NewPartitionManager(offset int64, generationID int32) *PartitionManager {
	return &PartitionManager{
		offset:            offset,
		generationID:      generationID,
		heap:              heap.NewHeap(),
		inProgress:        make(map[string]int64),
		meta:              make(map[string]struct{}),
		processedSearches: make(map[string]int64),
	}
}

// SetMeta sets meta information about searches that need to be skipped
func (pm *PartitionManager) SetMeta(meta map[string]struct{}) {
	pm.Lock()
	defer pm.Unlock()
	logging.WithField("meta", fmt.Sprint(meta)).Info("setting new meta")
	pm.meta = meta
}

func (pm *PartitionManager) GenerationID() int32 {
	pm.RLock()
	defer pm.RUnlock()
	return pm.generationID
}

func (pm *PartitionManager) ShouldSkipMessage(uuid string) bool {
	pm.RLock()
	defer pm.RUnlock()
	if _, ok := pm.meta[uuid]; ok {
		return true
	}
	return false
}

func (pm *PartitionManager) InitializeSearch(uuid string, offset int64) {
	pm.Lock()
	defer pm.Unlock()
	pm.inProgress[uuid] = offset
	pm.heap.Push(offset)
}

func (pm *PartitionManager) CloseSearch(uuid string) (int64, string, error) {
	pm.Lock()
	defer pm.Unlock()
	var offset int64
	var ok bool
	if offset, ok = pm.inProgress[uuid]; !ok {
		logging.Error("cannot find search uuid in close search operation")
		return -1, "", errors.New("cannot find search uuid in close search operation")
	}

	root := pm.heap.Peek()
	pm.processedSearches[uuid] = offset
	pm.heap.Remove(offset)
	delete(pm.inProgress, uuid)

	var searches []string
	if root == offset {
		for pUuid, pOffset := range pm.processedSearches {
			if ok && root >= pOffset {
				delete(pm.processedSearches, pUuid)
				continue
			}
			searches = append(searches, pUuid)
		}
		pm.offset = root
	} else {
		for uuid := range pm.processedSearches {
			searches = append(searches, uuid)
		}
	}
	return pm.offset, strings.Join(searches, ","), nil
}
