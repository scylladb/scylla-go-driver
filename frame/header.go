package frame

import (
	"fmt"
)

// Header spec https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L101.
type Header struct {
	Version  Byte
	Flags    Byte
	StreamID Short
	Opcode   OpCode
	Length   Int
}

func ParseHeader(b *Buffer) Header {
	h := Header{
		Version:  b.ReadByte(),
		Flags:    b.ReadFlags(),
		StreamID: b.ReadShort(),
		Opcode:   b.ReadOpCode(),
		Length:   b.ReadInt(),
	}
	// Currently, we only accept CQLv4 spec response frames.
	if h.Version != CQLv4 {
		b.RecordError(fmt.Errorf("invalid protocol version, only CQLv4 is accepted"))
	}
	return h
}

func (h Header) WriteTo(b *Buffer) {
	b.WriteByte(h.Version)
	b.WriteFlags(h.Flags)
	b.WriteShort(h.StreamID)
	b.WriteOpCode(h.Opcode)
	b.WriteInt(h.Length)
}
