package request

import (
	"bytes"
	"scylla-go-driver/frame"
)

// Prepare request message type.
type Prepare struct {
	Query string
}

// Write writes Prepare to the buffer.
func (p Prepare) WriteTo(b *bytes.Buffer) {
	frame.WriteLongString(p.Query, b)
}
