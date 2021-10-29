package frame

import "io"

type Header struct {
	version    Byte
	flags      Byte
	streamId   Short
	opcode     Byte
	length     Int
}

// NewHeader uses byte stream to construct Header,
// used when reading responses
func NewHeader(buf []byte) (*Header, error) {
	h := new(Header)

	v, err := ReadByte(buf)
	if err != nil {
		return nil, err
	}
	h.version = v

	f, err := ReadByte(buf)
	if err != nil {
		return h, err
	}
	h.flags = f

	sid, err := ReadShort(buf)
	if err != nil {
		return h, err
	}
	h.streamId = sid

	op, err := ReadByte(buf)
	if err != nil {
		return h, err
	}
	h.opcode = op

	l, err := ReadInt(buf)
	if err != nil {
		return h, err
	}
	h.length = l

	return h, nil
}

func (h *Header) WriteHeader(writer io.Writer) (int64, error) {
	wrote, err := WriteByte(byte(h.version), writer) // TODO are types even pretty?
	if err != nil {
		return wrote, err
	}

	l, err := WriteByte(byte(h.flags), writer)
	if err != nil {
		return wrote, err
	}
	wrote += l

	l, err = WriteShort(h.streamId, writer)
	if err != nil {
		return wrote, err
	}
	wrote += l

	l, err = WriteByte(byte(h.opcode), writer)
	if err != nil {
		return wrote, err
	}
	wrote += l

	l, err = WriteInt(h.length, writer)
	if err != nil {
		return wrote, err
	}
	wrote += l

	return wrote, nil
}