package frame

import (
	"bytes"
	"errors"
)

type Byte = byte
type Short = int16
type Int = int32
type Long = int64

type UUID = [16]byte
type StringMultiMap = map[string][]string
type StringList = []string

type OpCode = byte

type bufWrapper struct {
	buf *bytes.Buffer
	err *error
}

func (w bufWrapper) WriteByte(b Byte) {
	if *w.err == nil {
		w.buf.WriteByte(b)
	}
}

func (w bufWrapper) WriteShort(s Short) {
	if *w.err == nil {
		w.buf.Write([]byte{
			byte(s >> 8),
			byte(s),
		})
	}
}

func (w bufWrapper) WriteInt(i Int) {
	if *w.err == nil {
		w.buf.Write([]byte{
			byte(i >> 16),
			byte(i >> 8),
			byte(i),
		})
	}
}

func (w bufWrapper) WriteString(s string) {
	if *w.err == nil {
		// Write length of the string.
		w.WriteShort(Short(len(s)))

		w.buf.Write([]byte(s))
	}
}

func (w bufWrapper) WriteStringList(l StringList) {
	if *w.err == nil {
		// Write the length of the string list.
		w.WriteShort(Short(len(l)))

		// Write string list
		for _, s := range l {
			w.WriteString(s)
		}
	}
}

func (w bufWrapper) WriteStringMultiMap(m StringMultiMap) {
	if *w.err == nil {
		// Write the number of elements in the map.
		w.WriteShort(Short(len(m)))

		for k, l := range m {
			// Write key.
			w.WriteString(k)

			// Write value.
			w.WriteStringList(l)
		}
	}
}

func (w bufWrapper) ReadByte() Byte {
	if *w.err != nil {
		return 0
	}

	b, err := w.buf.ReadByte()
	*w.err = err

	return b
}

func (w bufWrapper) ReadShort() Short {
	return (Short(w.ReadByte()) << 8) | Short(w.ReadByte())
}

func (w bufWrapper) ReadInt() Int {
	return (Int(w.ReadShort()) << 8) | Int(w.ReadByte())
}

var bytesErr = errors.New("not enough bytes")

func (w bufWrapper) ReadString() string {
	// Read length of the string.
	if n := w.ReadShort(); *w.err == nil {
		b := make([]byte, n)
		cnt, err := w.buf.Read(b)
		*w.err = err

		if *w.err == nil && cnt != int(n) {
			*w.err = bytesErr
		} else {
			return string(b)
		}
	}

	return ""
}

func (w bufWrapper) ReadStringList() StringList {
	// Read the length of the string list.
	n := w.ReadShort()
	l := make(StringList, 0, n)

	for i := Short(0); i < n; i++ {
		// Read the strings and append them into list.
		s := w.ReadString()
		l = append(l, s)
	}

	return l
}

func (w bufWrapper) ReadStringMultiMap() StringMultiMap {
	// Read the number of elements in the map.
	n := w.ReadShort()
	m := StringMultiMap{}

	for i := Short(0); i < n; i++ {
		// Read the key.
		k := w.ReadString()

		// Read the value.
		l := w.ReadStringList()

		m[k] = l
	}

	return m
}

const (
	OpError         OpCode = 0x00
	OpStartup       OpCode = 0x01
	OpReady         OpCode = 0x02
	OpAuthenticate  OpCode = 0x03
	OpOptions       OpCode = 0x05
	OpSupported     OpCode = 0x06
	OpQuery         OpCode = 0x07
	OpResult        OpCode = 0x08
	OpPrepare       OpCode = 0x09
	OpExecute       OpCode = 0x0A
	OpRegister      OpCode = 0x0B
	OpEvent         OpCode = 0x0C
	OpBatch         OpCode = 0x0D
	OpAuthChallenge OpCode = 0x0E
	OpAuthResponse  OpCode = 0x0F
	OpAuthSuccess   OpCode = 0x10
)

type Header struct {
	Version  Byte
	Flags    Byte
	StreamID Short
	Opcode   OpCode
	Length   Int
}

var protocolVersionErr = errors.New("frame protocol version is not supported")

// ReadHeader uses bufWrapper  to construct Header.
// Used when reading responses.
func (w bufWrapper) ReadHeader() Header {
	v := w.ReadByte()
	f := w.ReadByte()
	s := w.ReadShort()
	o := w.ReadByte()
	l := w.ReadInt()

	h := Header{v, f, s, o, l}

	// Currently, we only accept CQLv4 spec response frames.
	if *w.err == nil && v != 0x84 {
		*w.err = protocolVersionErr
	}

	return h
}

func (w bufWrapper) WriteHeader(h Header) {
	w.WriteByte(h.Version)
	w.WriteByte(h.Flags)
	w.WriteShort(h.StreamID)
	w.WriteByte(h.Opcode)
	w.WriteInt(h.Length)
}

type Options struct {
	head Header
}

func NewOptions(h Header) Options {
	return Options{h}
}

func (w bufWrapper) WriteOptions(o Options) {
	w.WriteHeader(o.head)
}

type Supported struct {
	head    Header
	options StringMultiMap
}

// ReadSupported TODO checking whether we read right amount of bytes
func (w bufWrapper) ReadSupported(h Header) Supported {
	m := w.ReadStringMultiMap()

	return Supported{h, m}
}
