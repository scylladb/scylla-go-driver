package response

import (
	"scylla-go-driver/frame"
)

// Supported spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L537
type Supported struct {
	Options frame.StringMultiMap
}

func ParseSupported(b *frame.Buffer) Supported {
	return Supported{
		Options: b.ReadStringMultiMap(),
	}
}
