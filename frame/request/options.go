package request

import (
	"bytes"
)

// Options request message type.
type Options struct {}

// NewOptions creates and returns Options request.
func NewOptions() Options {
	return Options{}
}

// WriteOptions writes Options to the buffer.
func (Options) WriteTo(_ *bytes.Buffer) {}
