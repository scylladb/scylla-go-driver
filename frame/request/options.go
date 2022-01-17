package request

import (
	"scylla-go-driver/frame"
)

// Options spec https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L330.
type Options struct{}

func (*Options) WriteTo(_ *frame.Buffer) {}

func (*Options) OpCode() frame.OpCode {
	return frame.OpOptions
}
