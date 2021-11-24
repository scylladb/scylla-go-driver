package frame

import (
	"bytes"
	"fmt"
)

// Is holding both errors from reading (EOF) and errors from parsing.
type Buffer struct {
	Buf   bytes.Buffer
	Error error
}

func (b *Buffer) RecordError(err error) {
	if b.Error == nil {
		b.Error = err
	}
}

// Naming of Get and Put functions is arguable.
func (b *Buffer) PutByte(v Byte) {
	_ = b.Buf.WriteByte(v)
}

func (b *Buffer) PutShort(v Short) {
	_, _ = b.Buf.Write([]byte{
		byte(v >> 8),
		byte(v),
	})
}

func (b *Buffer) PutInt(v Int) {
	_, _ = b.Buf.Write([]byte{
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	})
}

func (b *Buffer) PutInet(v Inet) {
	// Writes length of the IP address.
	_ = b.Buf.WriteByte(Byte(len(v.IP)))
	_, _ = b.Buf.Write(v.IP)
	b.PutInt(v.Port)
}

func (b *Buffer) PutString(s string) {
	// Writes length of the string.
	b.PutShort(Short(len(s)))
	_, _ = b.Buf.WriteString(s)
}

func (b *Buffer) PutStringList(l StringList) {
	// Writes length of the string list.
	b.PutShort(Short(len(l)))
	// Writes consecutive strings.
	for _, s := range l {
		_, _ = b.Buf.WriteString(s)
	}
}

func (b *Buffer) GetByte() Byte {
	if b.Error == nil {
		var n Byte
		n, b.Error = b.Buf.ReadByte()
		return n
	}
	return Byte(0)
}

func (b *Buffer) GetShort() Short {
	if b.Error == nil {
		return Short(b.GetByte())<<8 | Short(b.GetByte())
	}
	return Short(0)
}

func (b *Buffer) GetInt() Int {
	if b.Error == nil {
		tmp := [4]byte{0, 0, 0, 0}
		_, b.Error = b.Buf.Read(tmp[:])
		return Int(tmp[0])<<24 |
			Int(tmp[1])<<16 |
			Int(tmp[2])<<8 |
			Int(tmp[3])
	}
	return Int(0)
}

// Get reads n bytes from the buffer.
func (b *Buffer) Get(n int) Bytes {
	if b.Error == nil {
		by := make(Bytes, n)
		_, b.Error = b.Buf.Read(by)
		return by
	}
	return nil
}

// If read Bytes length is negative returns nil.
func (b *Buffer) GetBytes() Bytes {
	if b.Error == nil {
		// Read length of the Bytes.
		n := b.GetInt()
		if n < 0 {
			return nil
		}
		return b.Get(int(n))
	}
	return nil
}

func (b *Buffer) GetInet() Inet {
	if b.Error == nil {
		var n Byte
		// Checks for valid length of the IP address.
		if n, b.Error = b.Buf.ReadByte(); n == 4 || n == 16 {
			return Inet{IP: b.Get(int(n)), Port: b.GetInt()}
		} else {
			b.RecordError(fmt.Errorf("invalid ip length"))
		}
	}
	return Inet{}
}

func (b *Buffer) GetString() string {
	if b.Error == nil {
		return string(b.Get(int(b.GetShort())))
	}
	return ""
}

func (b *Buffer) GetStringList() StringList {
	if b.Error == nil {
		// Read length of the string list.
		n := b.GetShort()
		l := make(StringList, n)
		for i := Short(0); i < n; i++ {
			// Read the strings and append them to the list.
			l = append(l, b.GetString())
		}
		return l
	}
	return StringList{}
}

func (b *Buffer) GetTopologyChangeType() TopologyChangeType {
	t := TopologyChangeType(b.GetString())
	if _, ok := topologyChangeTypes[t]; !ok {
		b.RecordError(fmt.Errorf("invalid TopologyChangeType: %s", t))
	}
	return t
}

func (b *Buffer) GetStatusChangeType() StatusChangeType {
	t := StatusChangeType(b.GetString())
	if _, ok := statusChangeTypes[t]; ok {
		b.RecordError(fmt.Errorf("invalid StatusChangeType: %s", t))
	}
	return t
}

func (b *Buffer) GetSchemaChangeType() SchemaChangeType {
	t := SchemaChangeType(b.GetString())
	if _, ok := schemaChangeTypes[t]; ok {
		b.RecordError(fmt.Errorf("invalid SchemaChangeType: %s", t))
	}
	return t
}

// Validation is not required. It is done inside SchemaChange event.
func (b *Buffer) GetSchemaChangeTarget() SchemaChangeTarget {
	return SchemaChangeTarget(b.GetString())
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
