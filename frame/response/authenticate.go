package response

import (
	"scylla-go-driver/frame"
)

// Authenticate spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L517
type Authenticate struct {
	Name string
}

func ParseAuthenticate(b *frame.Buffer) Authenticate {
	return Authenticate{
		Name: b.ReadString(),
	}
}
