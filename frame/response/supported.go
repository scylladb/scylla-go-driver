package response

import (
	"scylla-go-driver/frame"
)

// Supported response message type.
type Supported struct {
	Options frame.StringMultiMap
}

// ParseSupported reads and returns Supported from the buffer.
func ParseSupported(b *frame.Buffer) (Supported, error) {
	return Supported{
		Options: b.ReadStringMultiMap(),
	}, b.Error()
}
