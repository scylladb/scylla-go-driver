package response

import (
	"scylla-go-driver/frame"
)

// AuthSuccess spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L814
type AuthSuccess struct {
	Token frame.Bytes
}

func ParseAuthSuccess(b *frame.Buffer) (AuthSuccess, error) {
	return AuthSuccess{
		Token: b.ReadBytes(),
	}, b.Error()
}
