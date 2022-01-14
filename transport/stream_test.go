package transport

import (
	"math/rand"

	"scylla-go-driver/frame"
)

// TestStreamIDAllocator is StreamIDAllocator that returns a random.
type TestStreamIDAllocator struct{}

var _ StreamIDAllocator = TestStreamIDAllocator{}

func (t TestStreamIDAllocator) Alloc() (frame.StreamID, error) {
	const mask = int32(0xFFFF)
	return frame.StreamID(rand.Int31() & mask), nil
}

func (t TestStreamIDAllocator) Free(id frame.StreamID) {
}
