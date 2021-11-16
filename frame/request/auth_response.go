package request

import (
	"bytes"
	"scylla-go-driver/frame"
)

type AuthResponse struct {
	token frame.Bytes
}

// WriteStartup checks validity of given StringMap and
// if everything checks out then writes it into a buffer
func (a AuthResponse) Write(b *bytes.Buffer) {
	frame.WriteBytes(a.token, b)
}
