package request

import (
	"scylla-go-driver/frame"
)

// Options request message type.
type Options struct {
}

// WriteOptions writes Options to the buffer.
func (Options) WriteTo(_ *frame.Buffer) {}
