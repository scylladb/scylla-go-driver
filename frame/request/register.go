package request

import (
	"bytes"
	"scylla-go-driver/frame"
)

type Register struct {
	EventTypes frame.StringList
}

// WriteTo writes EventTypes into the bytes.Buffer.
func (r Register) WriteTo(b *bytes.Buffer) {
	frame.WriteStringList(r.EventTypes, b)
}
