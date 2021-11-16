package response

import (
	"bytes"
	"scylla-go-driver/frame"
)

type AuthSuccess struct {
	bytes frame.Bytes
}

func ReadAuthSuccess(b *bytes.Buffer) AuthSuccess {
	return AuthSuccess{bytes: frame.ReadBytes(b)}
}
