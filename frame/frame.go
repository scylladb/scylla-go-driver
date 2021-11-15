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

// WriteLong writes single Long to the buffer.
func WriteLong(l Long, b *bytes.Buffer) {
	b.Write([]byte{
		byte(l >> 56),
		byte(l >> 48),
		byte(l >> 40),
		byte(l >> 32),
		byte(l >> 24),
		byte(l >> 16),
		byte(l >> 8),
		byte(l),
	})
}

// WriteBytes writes Bytes to the buffer.
// If Bytes is nil then writes -1 to the buffer.
func WriteBytes(t Bytes, b *bytes.Buffer) {
	if t == nil {
		WriteInt(-1, b)
		return
	}

	// Writes length of the string list.
	WriteInt(Int(len(t)), b)
	// Writes consecutive strings.
	for _, s := range t {
		WriteByte(s, b)
	}
}

// WriteValue writes Value to the buffer.
func WriteValue(v Value, b *bytes.Buffer) {
	// Writes length of the value.
	WriteInt(v.N, b)
	// Writes value's body if there is any.
	if v.N > 0 {
		b.Write(v.Bytes)
	}
}

// WriteInet writes Inet to the buffer.
func WriteInet(i Inet, b *bytes.Buffer) {
	// Writes length of the IP address.
	WriteByte(Byte(len(i.IP)), b)
	b.Write(i.IP)
	WriteInt(i.Port, b)
}

// WriteString writes single string to the buffer.
func WriteString(s string, b *bytes.Buffer) {
	// Writes length of the string.
	WriteShort(Short(len(s)), b)
	b.WriteString(s)
}

// WriteLongString writes single long string to the buffer.
func WriteLongString(s string, b *bytes.Buffer) {
	// Writes length of the long string.
	WriteInt(Int(len(s)), b)
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

// WriteStringMap writes StringMap to the buffer.
func WriteStringMap(m StringMap, b *bytes.Buffer) {
	// Writes the number of elements in the map.
	WriteShort(Short(len(m)), b)
	// Writes consecutive map entries.
	for k, l := range m {
		// Writes key.
		WriteString(k, b)
		// Writes value.
		WriteString(l, b)
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
	tmp := [4]byte{0, 0, 0, 0}
	_, _ = b.Read(tmp[:])
	return Int(tmp[0])<<24 |
		Int(tmp[1])<<16 |
		Int(tmp[2])<<8 |
		Int(tmp[3])
}

// ReadLong reads and returns Long from the buffer.
func ReadLong(b *bytes.Buffer) Long {
	tmp := [8]byte{0, 0, 0, 0, 0, 0, 0, 0}
	_, _ = b.Read(tmp[:])
	return Long(tmp[0])<<56 |
		Long(tmp[1])<<48 |
		Long(tmp[2])<<40 |
		Long(tmp[3])<<32 |
		Long(tmp[4])<<24 |
		Long(tmp[5])<<16 |
		Long(tmp[6])<<8 |
		Long(tmp[7])
}

// ReadBytes reads Bytes from the buffer.
// If read Bytes length is negative returns nil.
func ReadBytes(b *bytes.Buffer) Bytes {
	// Reads length of the Bytes.
	n := ReadInt(b)
	if n < 0 {
		return nil
	}

	var out Bytes
	for i := Int(0); i < n; i++ {
		out = append(out, ReadByte(b))
	}
	return out
}

// ReadValue reads and return Value from the buffer.
// Length equal to -1 represents null.
// Length equal to -2 represents not set.
func ReadValue(b *bytes.Buffer) Value {
	// Reads length od the value.
	n := ReadInt(b)
	// Checks for valid length.
	if n < -2 {
		panic(invalidValueLength)
	}
	// Reads value's body if there is any.
	if n > 0 {
		tmp := make([]Byte, n)
		_, _ = b.Read(tmp)
		return Value{n, tmp}
	} else {
		return Value{N: n}
	}
}

// ReadInet reads and returns Inet from the buffer.
func ReadInet(b *bytes.Buffer) Inet {
	// Reads length of the IP address.
	n := ReadByte(b)
	// Checks for valid length of the IP address.
	if n != 4 && n != 16 {
		panic(invalidIPLength)
	}
	// Reads IP address.
	tmp := make([]Byte, n)
	_, _ = b.Read(tmp)
	return Inet{tmp, ReadInt(b)}
}

// ReadConsistency reads Short if it is valid consistency
// then returns it else panics.
func ReadConsistency(b *bytes.Buffer) Short {
	c := ReadShort(b)
	if c > 10 {
		panic(unknownConsistencyErr)
	}
	return c
}

var writeTypes = []string{
	"SIMPLE",
	"BATCH",
	"UNLOGGED_BATCH",
	"COUNTER",
	"BATCH_LOG",
	"CAS",
	"VIEW",
	"CDC",
}

// ReadWriteType reads string if it is valid write type
// then returns it else panics.
func ReadWriteType(b *bytes.Buffer) string {
	wt := ReadString(b)
	for _, v := range writeTypes {
		if wt == v {
			return wt
		}
	}
	panic(unknownWriteTypeErr)
}

// ReadString reads and returns string from the buffer.
func ReadString(b *bytes.Buffer) string {
	// Reads length of the string.
	n := ReadShort(b)
	// Placeholder for read Bytes.
	tmp := make([]byte, n)
	_, _ = b.Read(tmp)
	return string(tmp)
}

// ReadLongString reads and returns string from the buffer.
func ReadLongString(b *bytes.Buffer) string {
	// Reads length of the long string.
	n := ReadInt(b)
	// Placeholder for read Bytes.
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

// ReadStringMap reads and returns StringMap from the buffer.
func ReadStringMap(b *bytes.Buffer) StringMap {
	// Reads the number of elements in the map.
	n := ReadShort(b)
	m := StringMap{}
	for i := Short(0); i < n; i++ {
		// Reads the key.
		k := ReadString(b)
		// Reads the value.
		l := ReadString(b)
		m[k] = l
	}
	return m
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
