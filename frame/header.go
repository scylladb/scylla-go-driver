package frame

import (
	"fmt"
)

// StreamID is a type alias for Short.
type StreamID = Short

// HeaderSize specifies number of header bytes.
const HeaderSize = 9

// Header spec https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L101.
type Header struct {
	Version  Byte
	Flags    HeaderFlags
	StreamID Short
	OpCode   OpCode
	Length   Int
}

func ParseHeader(b *Buffer) Header {
	h := Header{
		Version:  b.ReadByte(),
		Flags:    b.ReadHeaderFlags(),
		StreamID: b.ReadShort(),
		OpCode:   b.ReadOpCode(),
		Length:   b.ReadInt(),
	}
	// Currently, we only accept CQLv4 spec response frames.
	if h.Version != CQLv4 {
		b.recordError(fmt.Errorf("invalid protocol version, only CQLv4 is accepted"))
	}
	return h
}

func (h Header) WriteTo(b *Buffer) {
	b.WriteByte(h.Version)
	b.WriteHeaderFlags(h.Flags)
	b.WriteShort(h.StreamID)
	b.WriteOpCode(h.OpCode)
	b.WriteInt(h.Length)
}
