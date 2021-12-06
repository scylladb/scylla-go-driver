package response

import (
	"scylla-go-driver/frame"
)

// AuthChallenge spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L802
type AuthChallenge struct {
	Token frame.Bytes
}

func ParseAuthChallenge(b *frame.Buffer) AuthChallenge {
	return AuthChallenge{
		Token: b.ReadBytes(),
	}
}
