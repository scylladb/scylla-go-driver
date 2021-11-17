package request

import (
	"bytes"
)

// Options request message type.
type Options struct {
}

// WriteOptions writes Options to the buffer.
func (Options) WriteTo(_ *bytes.Buffer) {}
