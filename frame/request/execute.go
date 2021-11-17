package request

import (
	"bytes"
	"scylla-go-driver/frame"
)

type Execute struct {
	ID frame.Bytes
	Options QueryOptions
}

// WriteTo writes Query ID and QueryOptions into the bytes.Buffer.
func (e Execute) WriteTo (b *bytes.Buffer) {
	_, _ = b.Write(e.ID)
	e.Options.WriteTo(b)
}