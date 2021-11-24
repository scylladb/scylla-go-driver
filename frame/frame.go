package frame

import (
	"bytes"
	"fmt"
)

type Buffer struct {
	buf   bytes.Buffer
	error error
}

func (b *Buffer) Error() error {
	return b.error
}

func (b *Buffer) RecordError(err error) {
	if b.error == nil {
		b.error = err
	}
}

func (b *Buffer) Write(v Bytes) {
	if b.error == nil {
		_, _ = b.buf.Write(v)
	}
}

func (b *Buffer) WriteByte(v Byte) {
	if b.error == nil {
		_ = b.buf.WriteByte(v)
	}
}

func (b *Buffer) WriteShort(v Short) {
	if b.error == nil {
		_, _ = b.buf.Write([]byte{
			byte(v >> 8),
			byte(v),
		})
	}
}

func (b *Buffer) WriteInt(v Int) {
	if b.error == nil {
		_, _ = b.buf.Write([]byte{
			byte(v >> 24),
			byte(v >> 16),
			byte(v >> 8),
			byte(v),
		})
	}
}

func (b *Buffer) WriteLong(v Long) {
	if b.error == nil {
		_, _ = b.buf.Write([]byte{
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
}

func (b *Buffer) WriteFlags(v Flags) {
	b.WriteByte(v)
}

func (b *Buffer) WriteOpCode(v OpCode) {
	if v > OpAuthSuccess {
		b.RecordError(fmt.Errorf("invalid operation code: %v", v))
	} else {
		b.WriteByte(v)
	}
}

func (b *Buffer) WriteConsistency(v Consistency) {
	// INVALID holds the biggest number among consistencies.
	if v > INVALID {
		b.RecordError(fmt.Errorf("invalid consistency: %v", v))
	} else {
		b.WriteShort(v)
	}
}

func (b *Buffer) WriteBytes(v Bytes) {
	if b.error == nil {
		if v == nil {
			b.WriteInt(-1)
		} else {
			// Writes length of the bytes.
			b.WriteInt(Int(len(v)))
			_, _ = b.buf.Write(v)
		}
	}
}

func (b *Buffer) WriteShortBytes(v Bytes) {
	if b.error == nil {
		// WriteTo length of the bytes.
		b.WriteShort(Short(len(v)))
		_, _ = b.buf.Write(v)
	}
}

func (b *Buffer) WriteValue(v Value) {
	b.WriteInt(v.N)
	// Writes value's body if there is any.
	if v.N > 0 {
		_, _ = b.buf.Write(v.Bytes)
	}
}

func (b *Buffer) WriteInet(v Inet) {
	if b.error == nil {
		if len(v.IP) != 4 && len(v.IP) != 16 {
			b.RecordError(fmt.Errorf("invalid IP length"))
		} else {
			// Writes length of the IP address.
			_ = b.buf.WriteByte(Byte(len(v.IP)))
			_, _ = b.buf.Write(v.IP)
			b.WriteInt(v.Port)
		}
	}
}

func (b *Buffer) WriteString(s string) {
	if b.error == nil {
		// Writes length of the string.
		b.WriteShort(Short(len(s)))
		_, _ = b.buf.WriteString(s)
	}
}

func (b *Buffer) WriteLongString(s string) {
	if b.error == nil {
		// Writes length of the long string.
		b.WriteInt(Int(len(s)))
		_, _ = b.buf.WriteString(s)
	}
}

func (b *Buffer) WriteStringList(l StringList) {
	// Writes length of the string list.
	b.WriteShort(Short(len(l)))
	// Writes consecutive strings.
	for _, s := range l {
		b.WriteString(s)
	}
}

func (b *Buffer) WriteStringMap(m StringMap) {
	// Writes the number of elements in the map.
	b.WriteShort(Short(len(m)))
	for k, v := range m {
		b.WriteString(k)
		b.WriteString(v)
	}
}

func (b *Buffer) WriteStringMultiMap(m StringMultiMap) {
	// Writes the number of elements in the map.
	b.WriteShort(Short(len(m)))
	// Writes consecutive map entries.
	for k, v := range m {
		// Writes key.
		b.WriteString(k)
		// Writes value.
		b.WriteStringList(v)
	}
}

// Reads n bytes from the buffer.
func (b *Buffer) Read(n int) Bytes {
	if b.error == nil {
		by := make(Bytes, n)
		_, b.error = b.buf.Read(by)
		return by
	}
	return nil
}

func (b *Buffer) ReadByte() Byte {
	if b.error == nil {
		var n Byte
		n, b.error = b.buf.ReadByte()
		return n
	}
	return Byte(0)
}

func (b *Buffer) ReadShort() Short {
	if b.error == nil {
		return Short(b.ReadByte())<<8 | Short(b.ReadByte())
	}
	return Short(0)
}

func (b *Buffer) ReadInt() Int {
	if b.error == nil {
		tmp := [4]byte{0, 0, 0, 0}
		_, b.error = b.buf.Read(tmp[:])
		return Int(tmp[0])<<24 |
			Int(tmp[1])<<16 |
			Int(tmp[2])<<8 |
			Int(tmp[3])
	}
	return Int(0)
}

func (b *Buffer) ReadLong() Long {
	if b.error == nil {
		tmp := [8]byte{0, 0, 0, 0, 0, 0, 0, 0}
		_, b.error = b.buf.Read(tmp[:])
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

func (b *Buffer) ReadOpCode() OpCode {
	o := b.ReadByte()
	// OpAuthSuccess holds the biggest number among operation codes.
	if o > OpAuthSuccess {
		b.RecordError(fmt.Errorf("invalid operation code: %v", o))
	}
	return o
}

func (b *Buffer) ReadFlags() Flags {
	return b.ReadByte()
}

// If read Bytes length is negative returns nil.
func (b *Buffer) ReadBytes() Bytes {
	if b.error == nil {
		// Read length of the Bytes.
		n := b.ReadInt()
		if n < 0 {
			return nil
		}
		return b.Read(int(n))
	}
	return nil
}

// If read Bytes length is negative returns nil.
func (b *Buffer) ReadShortBytes() Bytes {
	if b.error == nil {
		// Read length of the Bytes.
		n := b.ReadShort()
		return b.Read(int(n))
	}
	return nil
}

// Length equal to -1 represents null.
// Length equal to -2 represents not set.
func (b *Buffer) ReadValue() Value {
	if b.error == nil {
		if n := b.ReadInt(); n < -2 {
			b.RecordError(fmt.Errorf("invalid value length"))
		} else if n > 0 {
			return Value{N: n, Bytes: b.Read(int(n))}
		} else {
			return Value{N: n}
		}
	}
	return Value{}
}

func (b *Buffer) ReadInet() Inet {
	if b.error == nil {
		var n Byte
		// Checks for valid length of the IP address.
		if n, b.error = b.buf.ReadByte(); n == 4 || n == 16 {
			return Inet{IP: b.Read(int(n)), Port: b.ReadInt()}
		} else {
			b.RecordError(fmt.Errorf("invalid ip length"))
		}
	}
	return Inet{}
}

func (b *Buffer) ReadString() string {
	if b.error == nil {
		return string(b.Read(int(b.ReadShort())))
	}
	return ""
}

func (b *Buffer) ReadLongString() string {
	if b.error == nil {
		return string(b.Read(int(b.ReadInt())))
	}
	return ""
}

func (b *Buffer) ReadStringList() StringList {
	if b.error == nil {
		// Read length of the string list.
		n := b.ReadShort()
		l := make(StringList, n)
		for i := Short(0); i < n; i++ {
			// Read the strings and append them to the list.
			l = append(l, b.ReadString())
		}
		return l
	}
	return StringList{}
}

func (b *Buffer) ReadStringMap() StringMap {
	if b.error == nil {
		// Read the number of elements in the map.
		n := b.ReadShort()
		m := make(StringMap, n)
		for i := Short(0); i < n; i++ {
			k := b.ReadString()
			v := b.ReadString()
			m[k] = v
		}
		return m
	}
	return StringMap{}
}

func (b *Buffer) ReadStringMultiMap() StringMultiMap {
	if b.error == nil {
		// Read the number of elements in the map.
		n := b.ReadShort()
		m := make(StringMultiMap, n)
		for i := Short(0); i < n; i++ {
			k := b.ReadString()
			v := b.ReadStringList()
			m[k] = v
		}
		return m
	}
	return StringMultiMap{}
}

func contains(l StringList, s string) bool {
	for _, k := range l {
		if s == k {
			return true
		}
	}
	return false
}

func (b *Buffer) WriteStartupOptions(m StartupOptions) {
	if b.error == nil {
		count := 0
		for k, v := range mandatoryOptions {
			if s, ok := m[k]; !(ok && contains(v, s)) {
				b.RecordError(fmt.Errorf("invalid mandatory Startup option %s: %s", k, s))
				return
			} else {
				count = count + 1
			}
		}
		for k, v := range possibleOptions {
			if s, ok := m[k]; ok && !contains(v, s) {
				b.RecordError(fmt.Errorf("invalid Startup option %s: %s", k, s))
				return
			} else if ok {
				count = count + 1
			}
		}

		if count != len(m) {
			b.RecordError(fmt.Errorf("invalid Startup option"))
			return
		}

		b.WriteStringMap(m)
	}
}

func (b *Buffer) ParseTopologyChangeType() TopologyChangeType {
	t := TopologyChangeType(b.ReadString())
	if _, ok := topologyChangeTypes[t]; !ok {
		b.RecordError(fmt.Errorf("invalid TopologyChangeType: %s", t))
	}
	return t
}

func (b *Buffer) ParseStatusChangeType() StatusChangeType {
	t := StatusChangeType(b.ReadString())
	if _, ok := statusChangeTypes[t]; !ok {
		b.RecordError(fmt.Errorf("invalid StatusChangeType: %s", t))
	}
	return t
}

func (b *Buffer) ParseSchemaChangeType() SchemaChangeType {
	t := SchemaChangeType(b.ReadString())
	if _, ok := schemaChangeTypes[t]; !ok {
		b.RecordError(fmt.Errorf("invalid SchemaChangeType: %s", t))
	}
	return t
}

// Validation is not required. It is done inside SchemaChange event.
func (b *Buffer) ParseSchemaChangeTarget() SchemaChangeTarget {
	return SchemaChangeTarget(b.ReadString())
}
