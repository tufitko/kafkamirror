package heap

type Heap struct {
	buf []int64
}

func NewHeap() *Heap {
	return &Heap{
		buf: make([]int64, 0),
	}
}

// Push() adds value to the end of the heap and normalized the heap structure
func (h *Heap) Push(x int64) {
	h.buf = append(h.buf, x)
	h.up(len(h.buf) - 1)
}

// Peek() returns top value of heap
func (h *Heap) Peek() int64 {
	if len(h.buf) == 0 {
		return -1
	}
	return h.buf[0]
}

// Remove deletes value element from heap
func (h *Heap) Remove(x int64) {
	idx := -1
	for i, v := range h.buf {
		if v == x {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	h.buf[idx], h.buf[len(h.buf)-1] = h.buf[len(h.buf)-1], h.buf[idx]
	h.buf = h.buf[:len(h.buf)-1]
	h.down(idx)
}

func (h *Heap) up(idx int) {
	parent := (idx - 1) / 2
	if h.buf[parent] > h.buf[idx] {
		h.buf[parent], h.buf[idx] = h.buf[idx], h.buf[parent]
		h.up(parent)
	}
}

func (h *Heap) down(idx int) {
	leftChild := 2*idx + 1
	rightChild := 2*idx + 2

	switch {
	case rightChild < len(h.buf):
		if h.buf[rightChild] < h.buf[idx] && h.buf[rightChild] < h.buf[leftChild] {
			h.buf[idx], h.buf[rightChild] = h.buf[rightChild], h.buf[idx]
			h.down(rightChild)
		} else if h.buf[leftChild] < h.buf[idx] {
			h.buf[idx], h.buf[leftChild] = h.buf[leftChild], h.buf[idx]
			h.down(leftChild)
		}
	case leftChild < len(h.buf) && h.buf[leftChild] < h.buf[idx]:
		h.buf[idx], h.buf[leftChild] = h.buf[leftChild], h.buf[idx]
		h.down(leftChild)
	}
}
