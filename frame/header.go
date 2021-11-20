package frame

import "bytes"

// Header used both in requests and responses.
type Header struct {
	Version  Byte
	Flags    Byte
	StreamID Short
	Opcode   OpCode
	Length   Int
}

// ReadHeader reads and returns Header from the buffer.
// Used when handling responses.
func ReadHeader(b *Buffer) Header {
	h := Header{
		Version:  ReadByte(b),
		Flags:    ReadByte(b),
		StreamID: ReadShort(b),
		Opcode:   ReadByte(b),
		Length:   ReadInt(b),
	}
	// Currently, we only accept CQLv4 spec response frames.
	if h.Version != CQLv4 {
		panic(protocolVersionErr)
	}
	return h
}

// WriteHeader writes Header to the buffer.
// Used when handling requests.
func (h Header) Write(b *bytes.Buffer) {
	WriteByte(h.Version, b)
	WriteByte(h.Flags, b)
	WriteShort(h.StreamID, b)
	WriteByte(h.Opcode, b)
	WriteInt(h.Length, b)
}
