/*
Package request implements functions and types used for handling
all types of CQL binary protocol requests.
Writing to buffer is done in Big Endian order.
Request consists of frame.Header and body.
*/
package request

import (
	"bytes"
	"scylla-go-driver/frame"
)

// Options request message type.
type Options struct {
	head frame.Header
}

// NewOptions creates and returns Options request.
func NewOptions(h frame.Header) Options {
	return Options{h}
}

// WriteOptions writes Options to the buffer.
func WriteOptions(o Options, b *bytes.Buffer) {
	frame.WriteHeader(o.head, b)
}
