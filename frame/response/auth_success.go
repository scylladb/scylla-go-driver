package response

import (
	"bytes"
	"scylla-go-driver/frame"
)

type AuthSuccess struct {
	Bytes frame.Bytes
}

// ReadAuthSuccess reads CQL Bytes type into AuthSuccess.
func ReadAuthSuccess(b *bytes.Buffer) AuthSuccess {
	return AuthSuccess{Bytes: frame.ReadBytes(b)}
}
