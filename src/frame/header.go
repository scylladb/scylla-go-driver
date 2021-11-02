package frame

import (
	"errors"
)

type Header struct {
	Version  byte
	Flags    byte
	StreamID uint16
	Opcode   byte
	Length   int32
}

// ReadHeader uses byte stream to construct Header,
// used when reading responses
func ReadHeader(buf *[]byte) (*Header, error) {
	h := new(Header)

	var err error
	h.Version, err = ReadByte(buf)

	if err != nil {
		return nil, err
	}

	// Currently, we only accept CQLv4 spec response frames
	if h.Version != 0x84 {
		return nil, errors.New("frame protocol version is not supported")
	}

	h.Flags, err = ReadByte(buf)
	if err != nil {
		return nil, err
	}

	h.StreamID, err = ReadShort(buf)
	if err != nil {
		return nil, err
	}

	h.Opcode, err = ReadByte(buf)
	if err != nil {
		return nil, err
	}

	// Currently, we only support Supported frames.
	if h.Opcode != OpSupported {
		return nil, errors.New("frame opcode is not supported")
	}

	h.Length, err = ReadInt(buf)
	if err != nil {
		return h, err
	}

	return h, nil
}

func (h *Header) WriteTo(buf *[]byte) {
	WriteByte(h.Version, buf)
	WriteByte(h.Flags, buf)
	WriteShort(h.StreamID, buf)
	WriteByte(h.Opcode, buf)
	WriteInt(h.Length, buf)
}
