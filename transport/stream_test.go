package transport

import (
	"scylla-go-driver/frame"
	"testing"
)

func TestStreamIDAllocator(t *testing.T) {
	t.Parallel()
	s := streamIDAllocator{}

	allocated := make(map[frame.StreamID]struct{})

	// Allocate all possible non-negative streams.
	for i := 0; i <= maxStreamID; i++ {
		stream, err := s.Alloc()
		if err != nil {
			t.Fatalf("unable to get stream %d", i)
		}

		if _, ok := allocated[stream]; ok {
			t.Fatalf("got an already allocated stream %d", stream)
		}

		if stream != frame.StreamID(i) {
			t.Fatalf("expected stream %d, got stream %d", i, stream)
		}

		allocated[stream] = struct{}{}
	}

	// All streams are taken, we shouldn't be able to Alloc() another.
	if _, err := s.Alloc(); err == nil {
		t.Fatalf("allocating more than maxStreamID + 1 times in a row should fail")
	}

	// All streams are taken, so Alloc() after Free(x) should return streamID x.
	for key := range allocated {
		s.Free(key)
		if stream, err := s.Alloc(); err != nil {
			t.Fatalf("failed to reacquire stream %d", stream)
		} else if stream != key {
			t.Fatalf("expected stream %d, got stream %d", stream, key)
		}
	}
}
