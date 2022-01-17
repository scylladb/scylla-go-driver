package request

import (
	"scylla-go-driver/frame"
)

// Execute spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L403
type Execute struct {
	ID      frame.Bytes
	Options frame.QueryOptions
}

func (e *Execute) WriteTo(b *frame.Buffer) {
	b.WriteShortBytes(e.ID)
	b.WriteQueryOptions(e.Options)
}

func (*Execute) OpCode() frame.OpCode {
	return frame.OpExecute
}
