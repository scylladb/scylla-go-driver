package response

import (
	"bytes"

	"scylla-go-driver/frame"
)

// AuthChallenge response message type.
type AuthChallenge struct {
	Token frame.Bytes
}

// ReadAuthChallenge reads and returns AuthChallenge from the buffer.
func ReadAuthChallenge(b *bytes.Buffer) AuthChallenge {
	return AuthChallenge{frame.ReadBytes(b)}
}
