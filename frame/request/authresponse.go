package request

import (
	"scylla-go-driver/frame"
)

type AuthResponse struct {
	Token frame.Bytes
}

func (a AuthResponse) WriteTo(b *frame.Buffer) {
	b.WriteBytes(a.Token)
}
