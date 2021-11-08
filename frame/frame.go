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

func ReadByte(b *bytes.Buffer) Byte {
	n, _ := b.ReadByte()
	return n
}

func ReadShort(b *bytes.Buffer) Short {
	return (Short(ReadByte(b)) << 8) | Short(ReadByte(b))
}

func ReadInt(b *bytes.Buffer) Int {
	return (Int(ReadShort(b)) << 16) | Int(ReadShort(b))
}

func ReadString(b *bytes.Buffer) string {
	// Reads length of the string.
	n := ReadShort(b)
	// Placeholder for read bytes.
	tmp := make([]byte, n)
	b.Read(tmp)
	return string(tmp)
}

func ReadStringList(b *bytes.Buffer) StringList {
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

func ReadStringMultiMap(b *bytes.Buffer) StringMultiMap {
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
func ReadHeader(b *bytes.Buffer) Header {
	v := ReadByte(b)
	f := ReadByte(b)
	s := ReadShort(b)
	o := ReadByte(b)
	l := ReadInt(b)
	h := Header{v, f, s, o, l}
	// Currently, we only accept CQLv4 spec response frames.
	if v != CQLv4 {
		panic(protocolVersionErr)
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
