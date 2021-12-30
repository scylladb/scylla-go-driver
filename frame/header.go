package frame

import (
	"fmt"
)

// Header spec https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L101.
type Header struct {
	Version  Byte
	Flags    HeaderFlags
	StreamID Short
	Opcode   OpCode
	Length   Int
}

func ParseHeader(b *Buffer) Header {
	h := Header{
		Version:  b.ReadByte(),
		Flags:    b.ReadHeaderFlags(),
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

func ParseHeaderRaw(b []byte) (Header, error) {
	h := Header{
		Version:  b[0],
		Flags:    b[1],
		StreamID: Short(b[2])<<8 | Short(b[3]),
		Opcode:   b[4],
		Length: Int(b[5])<<24 |
			Int(b[6])<<16 |
			Int(b[7])<<8 |
			Int(b[8]),
	}

	if h.Version != CQLv4 {
		return h, fmt.Errorf("invalid CQL version: %v", h.Version)
	}

	if h.Opcode > OpAuthSuccess {
		return h, fmt.Errorf("invalid operation code: %v", h.Opcode)
	}

	return h, nil
}

func (h Header) WriteTo(b *Buffer) {
	b.WriteByte(h.Version)
	b.WriteHeaderFlags(h.Flags)
	b.WriteShort(h.StreamID)
	b.WriteOpCode(h.Opcode)
	b.WriteInt(h.Length)
}
