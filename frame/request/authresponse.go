package request

import (
	"scylla-go-driver/frame"
)

// AuthResponse spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L311
type AuthResponse struct {
	Token frame.Bytes
}

func (a AuthResponse) WriteTo(b *frame.Buffer) {
	b.WriteBytes(a.Token)
}

func (AuthResponse) OpCode() frame.OpCode {
	return frame.OpAuthResponse
}
