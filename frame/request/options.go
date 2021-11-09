// Package request implements functions and types used for handling
// all types of CQL binary protocol requests.
// Writing to buffer is done in Big Endian order.

package request

import (
	"bytes"
)

// Options request message type.
type Options struct {

}

// NewOptions creates and returns Options request.
func NewOptions() Options {
	return Options{}
}

// WriteOptions writes Options to the buffer.
func WriteOptions(_ Options, _ *bytes.Buffer) {}
