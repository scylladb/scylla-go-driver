package response

import (
	"bytes"
	"scylla-go-driver/frame"
)

// Supported response message type.
type Supported struct {
	options frame.StringMultiMap
}

// ReadSupported reads and returns Supported from the buffer.
func ReadSupported(b *bytes.Buffer) Supported {
	m := frame.ReadStringMultiMap(b)
	return Supported{m}
}
