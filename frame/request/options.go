package request

import (
	"github.com/mmatczuk/scylla-go-driver/frame"
)

var _ frame.Request = (*Options)(nil)

// Options spec https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L330.
type Options struct{}

func (*Options) WriteTo(_ *frame.Buffer) {}

func (*Options) OpCode() frame.OpCode {
	return frame.OpOptions
}
