package response

import (
	"scylla-go-driver/frame"
)

// Ready spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L507
type Ready struct{}

func ParseReady(_ *frame.Buffer) *Ready {
	return &Ready{}
}
