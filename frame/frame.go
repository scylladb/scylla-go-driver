package frame

import (
	"bytes"
)

func WriteByte(n Byte, b *bytes.Buffer) {
	b.WriteByte(n)
}

func WriteShort(s Short, b *bytes.Buffer) {
	b.Write([]byte{
		byte(s >> 8),
		byte(s),
	})
}

func WriteInt(i Int, b *bytes.Buffer) {
	b.Write([]byte{
		byte(i >> 24),
		byte(i >> 16),
		byte(i >> 8),
		byte(i),
	})
}

func WriteString(s string, b *bytes.Buffer) {
	// Writes length of the string.
	WriteShort(Short(len(s)), b)
	b.WriteString(s)
}

func WriteStringList(l StringList, b *bytes.Buffer) {
	// Writes length of the string list.
	WriteShort(Short(len(l)), b)
	// Writes the string list.
	for _, s := range l {
		WriteString(s, b)
	}
}

func WriteStringMultiMap(m StringMultiMap, b *bytes.Buffer) {
	// Writes the number of elements in the map.
	WriteShort(Short(len(m)), b)
	for k, l := range m {
		// Write key.
		WriteString(k, b)
		// Write value.
		WriteStringList(l, b)
	}
}

func ReadByte(b Buffer) Byte {
	if *b.Err != nil {
		return 0
	}
	n, err := b.Buf.ReadByte()
	*b.Err = err
	return n
}

func ReadShort(b Buffer) Short {
	if *b.Err != nil {
		return 0
	}
	// TODO: can we allocate byte slice for two elements?
	return (Short(ReadByte(b)) << 8) | Short(ReadByte(b))
}

func ReadInt(b Buffer) Int {
	return (Int(ReadShort(b)) << 16) | Int(ReadShort(b))
}

func ReadString(b Buffer) string {
	// Reads length of the string.
	if n := ReadShort(b); *b.Err == nil {
		// Placeholder for read bytes.
		tmp := make([]byte, n)
		cnt, err := b.Buf.Read(tmp)
		*b.Err = err

		// Checks the amount of read bytes.
		if *b.Err == nil && cnt != int(n) {
			*b.Err = bytesErr
		} else {
			return string(tmp)
		}
	}
	return ""
}

func ReadStringList(b Buffer) StringList {
	// Reads length of the string list.
	n := ReadShort(b)
	l := make(StringList, 0, n)
	for i := Short(0); i < n; i++ {
		// Read the strings and append them into list.
		s := ReadString(b)
		l = append(l, s)
	}
	return l
}

func ReadStringMultiMap(b Buffer) StringMultiMap {
	// Reads the number of elements in the map.
	n := ReadShort(b)
	m := StringMultiMap{}
	for i := Short(0); i < n; i++ {
		// Read the key.
		k := ReadString(b)
		// Read the value.
		l := ReadStringList(b)
		m[k] = l
	}
	return m
}

// ReadHeader uses Buffer to construct Header.
// Used when reading responses.
func ReadHeader(b Buffer) Header {
	v := ReadByte(b)
	f := ReadByte(b)
	s := ReadShort(b)
	o := ReadByte(b)
	l := ReadInt(b)
	h := Header{v, f, s, o, l}
	// Currently, we only accept CQLv4 spec response frames.
	if *b.Err == nil && v != 0x84 {
		*b.Err = protocolVersionErr
	}
	return h
}

func WriteHeader(h Header, b *bytes.Buffer) {
	WriteByte(h.Version, b)
	WriteByte(h.Flags, b)
	WriteShort(h.StreamID, b)
	WriteByte(h.Opcode, b)
	WriteInt(h.Length, b)
}
