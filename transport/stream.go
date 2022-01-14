package transport

import (
	"scylla-go-driver/frame"
)

type StreamIDAllocator interface {
	Alloc() (frame.StreamID, error)
	Free(id frame.StreamID)
}
