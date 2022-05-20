package stream

import (
	"hash"
	"hash/fnv"
	"sync"

	"github.com/tufitko/kafkamirror/pkg/logging"
)

// Pool provides partition message pool.
type Pool struct {
	partitions []*Partition
	poolSize   int
	bufferSize int
	hasher     hash.Hash32
	mu         sync.Mutex
}

// Partition contains channel of messages as well as sync object in order to shutdown gracefully
type Partition struct {
	messages chan *Message
	once     sync.Once
}

// NewPool creates new pool.
func NewPool(size, buffer int) *Pool {
	p := &Pool{
		poolSize:   size,
		bufferSize: buffer,
		hasher:     fnv.New32a(),
		partitions: make([]*Partition, size),
	}

	for i := 0; i < p.poolSize; i++ {
		p.partitions[i] = &Partition{
			messages: make(chan *Message, p.bufferSize),
		}
	}

	return p
}

// Add puts message to specific partition.
func (p *Pool) Add(msg *Message) {
	p.partitions[p.partitionID(msg.Key)].messages <- msg
}

// Partition returns partition specific channel.
func (p *Pool) Partition(i int) chan *Message {
	return p.partitions[i].messages
}

// Close closes all partitions channels.
func (p *Pool) Close() {
	for _, partition := range p.partitions {
		p.SafeClose(partition)
	}
}

func (p *Pool) SafeClose(partition *Partition) {
	partition.once.Do(func() {
		close(partition.messages)
	})
}

func (p *Pool) hash(data string) uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.hasher.Reset()
	_, err := p.hasher.Write([]byte(data))
	if err != nil {
		logging.WithError(err).Error("pool: hasher write error")
	}

	return p.hasher.Sum32()
}

func (p *Pool) partitionID(key string) uint32 {
	return p.hash(key) % uint32(p.poolSize)
}
