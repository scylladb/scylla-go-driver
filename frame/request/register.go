package request

import (
	"scylla-go-driver/frame"
)

// Register spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L477
type Register struct {
	EventTypes []frame.EventType
}

func (r *Register) WriteTo(b *frame.Buffer) {
	b.WriteEventTypes(r.EventTypes)
}

func (*Register) OpCode() frame.OpCode {
	return frame.OpRegister
}
