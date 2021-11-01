package frame

import "io"

type Header struct {
	Version  byte
	Flags    byte
	StreamID uint16
	Opcode   byte
	Length   int32
}

// NewHeader uses byte stream to construct Header,
// used when reading responses
func NewHeader(buf *[]byte) (*Header, error) {
	h := new(Header)

	v, err := ReadByte(buf)
	if err != nil {
		return nil, err
	}
	h.Version = v

	f, err := ReadByte(buf)
	if err != nil {
		return h, err
	}
	h.Flags = f

	sid, err := ReadShort(buf)
	if err != nil {
		return h, err
	}
	h.StreamID = sid

	op, err := ReadByte(buf)
	if err != nil {
		return h, err
	}
	h.Opcode = op

	l, err := ReadInt(buf)
	if err != nil {
		return h, err
	}
	h.Length = l

	return h, nil
}

func (h *Header) WriteHeader(writer io.Writer) (int64, error) {
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
