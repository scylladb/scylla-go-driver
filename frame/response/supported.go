package response

import (
	"bytes"

	"scylla-go-driver/frame"
)

// Supported response message type.
type Supported struct {
	Options frame.StringMultiMap
}

// ReadSupported reads and returns Supported from the buffer.
func ReadSupported(b *bytes.Buffer) Supported {
	return Supported{Options: frame.ReadStringMultiMap(b)}
}
