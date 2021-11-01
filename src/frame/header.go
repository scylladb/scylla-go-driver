package frame

import (
	"errors"
	"io"
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
	if len(*buf) < 9 {
		return nil, errors.New("buffer too short to read header")
	}

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

func (h *Header) Write(writer io.Writer) (int64, error) {
	wrote, err := WriteByte(h.Version, writer)
	if err != nil {
		return wrote, err
	}

	l, err := WriteByte(h.Flags, writer)
	if err != nil {
		return wrote, err
	}
	wrote += l

	l, err = WriteShort(h.StreamID, writer)
	if err != nil {
		return wrote, err
	}
	wrote += l

	l, err = WriteByte(h.Opcode, writer)
	if err != nil {
		return wrote, err
	}
	wrote += l

	l, err = WriteInt(h.Length, writer)
	if err != nil {
		return wrote, err
	}
	wrote += l

	return wrote, nil
}
