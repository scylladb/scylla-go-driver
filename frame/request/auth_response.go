package request

import (
	"bytes"
	"scylla-go-driver/frame"
)

type AuthResponse struct {
	Token frame.Bytes
}

// WriteTo writes Token from AuthResponse into the bytes.Buffer.
func (a AuthResponse) WriteTo(b *bytes.Buffer) {
	frame.WriteBytes(a.Token, b)
}
