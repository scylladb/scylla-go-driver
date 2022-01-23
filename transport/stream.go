package transport

import (
	"fmt"
	"math"
	"math/bits"
	"scylla-go-driver/frame"
)

type StreamIDAllocator interface {
	Alloc() (frame.StreamID, error)
	Free(id frame.StreamID)
}

const maxStreamID = math.MaxInt16

const bucketSize = 64
const buckets = (maxStreamID + 1) / bucketSize

// DefaultStreamIDAllocator is a StreamIDAllocator that always allocates the smallest possible stream on Alloc().
type DefaultStreamIDAllocator struct {
	usedBitmap [buckets]uint64
}

func (s *DefaultStreamIDAllocator) Alloc() (frame.StreamID, error) {
	for blockID, block := range &s.usedBitmap {
		if block < math.MaxUint64 {
			offset := bits.TrailingZeros64(^block)
			s.usedBitmap[blockID] |= 1 << offset
			return frame.StreamID(offset + blockID*bucketSize), nil
		}
	}
	return 0, fmt.Errorf("stream ID alloc: all stream ID's are busy")
}

func (s *DefaultStreamIDAllocator) Free(id frame.StreamID) {
	blockID := id / bucketSize
	offset := id % bucketSize
	s.usedBitmap[blockID] ^= 1 << offset
}
