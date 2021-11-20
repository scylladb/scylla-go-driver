package frame

import (
	"bytes"
	"fmt"
)

type Buffer struct {
	bytes.Buffer
	error error
}

func (b *Buffer) RecordError(err error) {
	b.error = err
}

func (b *Buffer) PutByte(v Byte) {
	_ = b.WriteByte(v)
}

func (b *Buffer) PutShort(v Short) {
	_, _ = b.Write([]byte{
		byte(v >> 8),
		byte(v),
	})
}

func (b *Buffer) PutInt(v Int) {
	_, _ = b.Write([]byte{
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	})
}

func (b *Buffer) PutLong(v Long) {
	_, _ = b.Write([]byte{
		byte(v >> 56),
		byte(v >> 48),
		byte(v >> 40),
		byte(v >> 32),
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	})
}

// PutBytes writes Bytes to the bytes.buffer,
// if v is nil then writes -1 to the buffer.
func (b *Buffer) PutBytes(v Bytes) {
	if v == nil {
		b.PutInt(-1)
	} else {
		// Write length of the bytes.
		b.PutInt(Int(len(v)))
		_, _ = b.Write(v)
	}
}

func (b *Buffer) PutShortBytes(v Bytes) {
	// Write length of the bytes.
	b.PutShort(Short(len(v)))
	_, _ = b.Write(v)
}

func (b *Buffer) PutValue(v Value) {
	// Write length of the value.
	b.PutInt(v.N)
	// Writes value's body if there is any.
	if v.N > 0 {
		_, _ = b.Write(v.Bytes)
	}
}

func (b *Buffer) PutInet(v Inet) {
	// Writes length of the IP address.
	_ = b.WriteByte(Byte(len(v.IP)))
	_, _ = b.Write(v.IP)
	b.PutInt(v.Port)
}

func (b *Buffer) PutString(s string) {
	// Writes length of the string.
	b.PutShort(Short(len(s)))
	_, _ = b.WriteString(s)
}

func (b *Buffer) PutLongString(s string) {
	// Writes length of the long string.
	b.PutInt(Int(len(s)))
	_, _ = b.WriteString(s)
}

func (b *Buffer) PutStringList(l StringList) {
	// Writes length of the string list.
	b.PutShort(Short(len(l)))
	// Writes consecutive strings.
	for _, s := range l {
		_, _ = b.WriteString(s)
	}
}

func (b *Buffer) PutStringMap(m StringMap) {
	// Writes the number of elements in the map.
	b.PutShort(Short(len(m)))
	for k, v := range m {
		_, _ = b.WriteString(k)
		_, _ = b.WriteString(v)
	}
}

func (b *Buffer) PutStringMultiMap(m StringMultiMap) {
	// Writes the number of elements in the map.
	b.PutShort(Short(len(m)))

	// Writes consecutive map entries.
	for k, v := range m {
		// Writes key.
		_, _ = b.WriteString(k)
		// Writes value.
		b.PutStringList(v)
	}
}

// Get reads n bytes from the buffer.
func (b *Buffer) Get(n int) Bytes {
	if b.error == nil {
		by := make(Bytes, n)
		_, b.error = b.Read(by)
		return by
	}
	return nil
}

func (b *Buffer) GetByte() Byte {
	if b.error == nil {
		var n Byte
		n, b.error = b.ReadByte()
		return n
	}
	return Byte(0)
}

func (b *Buffer) GetShort() Short {
	if b.error == nil {
		return Short(b.GetByte())<<8 | Short(b.GetByte())
	}
	return Short(0)
}

func (b *Buffer) GetInt() Int {
	if b.error == nil {
		tmp := [4]byte{0, 0, 0, 0}
		_, b.error = b.Read(tmp[:])
		return Int(tmp[0])<<24 |
			Int(tmp[1])<<16 |
			Int(tmp[2])<<8 |
			Int(tmp[3])
	}
	return Int(0)
}

func (b *Buffer) GetLong() Long {
	if b.error == nil {
		tmp := [8]byte{0, 0, 0, 0, 0, 0, 0, 0}
		_, b.error = b.Read(tmp[:])
		return Long(tmp[0])<<56 |
			Long(tmp[1])<<48 |
			Long(tmp[2])<<40 |
			Long(tmp[3])<<32 |
			Long(tmp[4])<<24 |
			Long(tmp[5])<<16 |
			Long(tmp[6])<<8 |
			Long(tmp[7])
	}
	return Long(0)
}

// If read Bytes length is negative returns nil.
func (b *Buffer) GetBytes() Bytes {
	if b.error == nil {
		// Read length of the Bytes.
		n := b.GetInt()
		if n < 0 {
			return nil
		}
		return b.Get(int(n))
	}
	return nil
}

// If read Bytes length is negative returns nil.
func (b *Buffer) GetShortBytes() Bytes {
	if b.error == nil {
		// Read length of the Bytes.
		n := b.GetShort()
		return b.Get(int(n))
	}
	return nil
}

// GetValue reads Value from the buffer.
// Length equal to -1 represents null.
// Length equal to -2 represents not set.
func (b *Buffer) GetValue() Value {
	if b.error == nil {
		if n := b.GetInt(); n < -2 {
			b.RecordError(fmt.Errorf("invalid value length"))
		} else if n > 0 {
			return Value{N: n, Bytes: b.Get(int(n))}
		} else {
			return Value{N: n}
		}
	}
	return Value{}
}

func (b *Buffer) GetInet() Inet {
	if b.error == nil {
		var n Byte
		// Checks for valid length of the IP address.
		if n, b.error = b.ReadByte(); n == 4 || n == 16 {
			return Inet{IP: b.Get(int(n)), Port: b.GetInt()}
		} else {
			b.RecordError(fmt.Errorf("invalid ip length"))
		}
	}
	return Inet{}
}

func (b *Buffer) GetConsistency() Short {
	if b.error == nil {
		if n := Short(b.GetByte())<<8 | Short(b.GetByte()); n < INVALID {
			return n
		}
		b.RecordError(fmt.Errorf("unknown consistency"))
	}
	return ANY
}

// GetWriteType reads string. Returns if it is a valid write type, else records error onto Buffer.
func (b *Buffer) GetWriteType() WriteType {
	if b.error == nil {
		t := b.Get(int(b.GetShort()))
		var w WriteType
		if err := w.UnMarshalText(t); err != nil {
			b.RecordError(err)
		} else {
			return w
		}
	}
	return ""
}

func (b *Buffer) GetString() string {
	if b.error == nil {
		return string(b.Get(int(b.GetShort())))
	}
	return ""
}

func (b *Buffer) GetLongString() string {
	if b.error == nil {
		return string(b.Get(int(b.GetInt())))
	}
	return ""
}

func (b *Buffer) GetStringList() StringList {
	if b.error == nil {
		// Read length of the string list.
		n := b.GetShort()
		l := make(StringList, n)
		for i := Short(0); i < n; i++ {
			// Read the strings and append them to the list.
			s := b.GetString()
			l = append(l, s)
		}
		return l
	}
	return StringList{}
}

func (b *Buffer) GetStringMap() StringMap {
	if b.error == nil {
		// Read the number of elements in the map.
		n := b.GetShort()
		m := make(StringMap, n)
		for i := Short(0); i < n; i++ {
			k := b.GetString()
			v := b.GetString()
			m[k] = v
		}
		return m
	}
	return StringMap{}
}

func (b *Buffer) GetStringMultiMap() StringMultiMap {
	if b.error == nil {
		// Read the number of elements in the map.
		n := b.GetShort()
		m := make(StringMultiMap, n)
		for i := Short(0); i < n; i++ {
			k := b.GetString()
			v := b.GetStringList()
			m[k] = v
		}
		return m
	}
	return StringMultiMap{}
}

func (t *WriteType) MarshalText() ([]byte, error) {
	l := Short(len(string(*t)))
	// Append WriteType body, which is string to its length.
	return append([]byte{byte(l >> 8), byte(l)}, string(*t)...), nil
}

func (t *WriteType) UnMarshalText(b []byte) error {
	// Length validation depends on flow I don't know yet.
	// Let's assume that we don't need to validate length of string - there is pure string in the slice, without length.
	// TODO: resolve this in the future.

	*t = WriteType(b)
	if _, ok := ValidWriteTypes[*t]; ok {
		return nil
	} else {
		return fmt.Errorf("unknown WriteType")
	}
}
