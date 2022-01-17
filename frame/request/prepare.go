package request

import (
	"scylla-go-driver/frame"
)

// Prepare spec https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L394.
type Prepare struct {
	Query string
}

func (p *Prepare) WriteTo(b *frame.Buffer) {
	b.WriteLongString(p.Query)
}

func (*Prepare) OpCode() frame.OpCode {
	return frame.OpPrepare
}
