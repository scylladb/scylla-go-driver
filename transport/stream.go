package transport

import (
	"fmt"
	"math"
	"math/bits"

	"scylla-go-driver/frame"
)

const (
	maxStreamID = math.MaxInt16

	bucketSize = 64
	buckets    = (maxStreamID + 1) / bucketSize
)

type streamIDAllocator struct {
	usedBitmap [buckets]uint64
}

// Alloc() allocates the smallest possible stream ID on Alloc().
func (s *streamIDAllocator) Alloc() (frame.StreamID, error) {
	for blockID, block := range &s.usedBitmap {
		if block < math.MaxUint64 {
			offset := bits.TrailingZeros64(^block)
			s.usedBitmap[blockID] |= 1 << offset
			return frame.StreamID(offset + blockID*bucketSize), nil
		}
	}
	return 0, fmt.Errorf("all stream IDs are busy")
}

func (s *streamIDAllocator) Free(id frame.StreamID) {
	blockID := id / bucketSize
	offset := id % bucketSize
	s.usedBitmap[blockID] ^= 1 << offset
}
