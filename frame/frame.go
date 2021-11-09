// Package frame implements generic functions for
// reading and writing types from CQL binary protocol.
// This implementation DOES NOT handle any type of errors
// thrown because of reading or writing the buffer.
// In case of any, the panic is invoked.
// Reading and writing is done in Big Endian order.

package frame

import (
	"bytes"
)

// WriteByte writes single Byte to the buffer.
func WriteByte(n Byte, b *bytes.Buffer) {
	b.WriteByte(n)
}

// WriteShort writes single Short to the buffer.
func WriteShort(s Short, b *bytes.Buffer) {
	b.Write([]byte{
		byte(s >> 8),
		byte(s),
	})
}

// WriteInt writes single Int to the buffer.
func WriteInt(i Int, b *bytes.Buffer) {
	b.Write([]byte{
		byte(i >> 24),
		byte(i >> 16),
		byte(i >> 8),
		byte(i),
	})
}

// WriteString writes single string to the buffer.
func WriteString(s string, b *bytes.Buffer) {
	// Writes length of the string.
	WriteShort(Short(len(s)), b)
	b.WriteString(s)
}

// WriteStringList writes StringList to the buffer.
func WriteStringList(l StringList, b *bytes.Buffer) {
	// Writes length of the string list.
	WriteShort(Short(len(l)), b)
	// Writes consecutive strings.
	for _, s := range l {
		WriteString(s, b)
	}
}

// WriteStringMultiMap writes StringMultiMap to the buffer.
func WriteStringMultiMap(m StringMultiMap, b *bytes.Buffer) {
	// Writes the number of elements in the map.
	WriteShort(Short(len(m)), b)
	// Writes consecutive map entries.
	for k, l := range m {
		// Writes key.
		WriteString(k, b)
		// Writes value.
		WriteStringList(l, b)
	}
}

// ReadByte reads and returns next Byte from the buffer.
func ReadByte(b *bytes.Buffer) Byte {
	n, _ := b.ReadByte()
	return n
}

// ReadShort reads and returns Short from the buffer.
func ReadShort(b *bytes.Buffer) Short {
	return Short(ReadByte(b))<<8 | Short(ReadByte(b))
}

// ReadInt reads and returns Int from the buffer.
func ReadInt(b *bytes.Buffer) Int {
	tmp := []byte{0, 0, 0, 0}
	_, _ = b.Read(tmp)
	return Int(tmp[0])<<24 |
		Int(tmp[1])<<16 |
		Int(tmp[2])<<8 |
		Int(tmp[3])
}

// ReadString reads and returns string from the buffer.
func ReadString(b *bytes.Buffer) string {
	// Reads length of the string.
	n := ReadShort(b)
	// Placeholder for read bytes.
	tmp := make([]byte, n)
	_, _ = b.Read(tmp)
	return string(tmp)
}

// ReadStringList reads and returns StringList from the buffer.
func ReadStringList(b *bytes.Buffer) StringList {
	// Reads length of the string list.
	n := ReadShort(b)
	l := StringList{}
	for i := Short(0); i < n; i++ {
		// Reads the strings and append them to the list.
		s := ReadString(b)
		l = append(l, s)
	}
	return l
}

// ReadStringMultiMap reads and returns StringMultiMap from the buffer.
func ReadStringMultiMap(b *bytes.Buffer) StringMultiMap {
	// Reads the number of elements in the map.
	n := ReadShort(b)
	m := StringMultiMap{}
	for i := Short(0); i < n; i++ {
		// Reads the key.
		k := ReadString(b)
		// Reads the value.
		l := ReadStringList(b)
		m[k] = l
	}
	return m
}

// ReadHeader reads and returns Header from the buffer.
// Used when handling responses.
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

// WriteHeader writes Header to the buffer.
// Used when handling requests.
func WriteHeader(h Header, b *bytes.Buffer) {
	WriteByte(h.Version, b)
	WriteByte(h.Flags, b)
	WriteShort(h.StreamID, b)
	WriteByte(h.Opcode, b)
	WriteInt(h.Length, b)
}
