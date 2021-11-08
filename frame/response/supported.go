/*
Package response implements functions and types used for handling
all types of CQL binary protocol responses.
Reading from buffer is done in Big Endian order.
Response consists of frame.Header and body.
*/
package response

import (
	"bytes"
	"scylla-go-driver/frame"
)

// Supported response message type.
type Supported struct {
	head    frame.Header
	options frame.StringMultiMap
}

// ReadSupported reads and returns Supported from the buffer.
func ReadSupported(h frame.Header, b *bytes.Buffer) Supported {
	m := frame.ReadStringMultiMap(b)
	return Supported{h, m}
}
