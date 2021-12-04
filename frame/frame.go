package frame

import (
	"bytes"
	"fmt"
)

type Buffer struct {
	buf bytes.Buffer
	err error
}

func (b *Buffer) Error() error {
	return b.err
}

func (b *Buffer) RecordError(err error) {
	if b.err == nil {
		b.err = err
	}
}

func (b *Buffer) Write(v Bytes) {
	if b.err == nil {
		_, _ = b.buf.Write(v)
	}
}

func (b *Buffer) WriteByte(v Byte) {
	if b.err == nil {
		_ = b.buf.WriteByte(v)
	}
}

func (b *Buffer) WriteShort(v Short) {
	if b.err == nil {
		_, _ = b.buf.Write([]byte{
			byte(v >> 8),
			byte(v),
		})
	}
}

func (b *Buffer) WriteInt(v Int) {
	if b.err == nil {
		_, _ = b.buf.Write([]byte{
			byte(v >> 24),
			byte(v >> 16),
			byte(v >> 8),
			byte(v),
		})
	}
}

func (b *Buffer) WriteLong(v Long) {
	if b.err == nil {
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
	if b.err == nil {
		b.WriteByte(v)
	}
}

func (b *Buffer) WriteOpCode(v OpCode) {
	if b.err == nil {
		if _, ok := ValidOpCodes[v]; ok {
			b.WriteByte(v)
		} else {
			b.RecordError(fmt.Errorf("invalid operation code: %v", v))
		}
	}
}

func (b *Buffer) WriteConsistency(v Consistency) {
	if b.err == nil {
		// INVALID holds the biggest number among consistencies.
		if v >= InvalidConsistency {
			b.RecordError(fmt.Errorf("invalid consistency: %v", v))
		} else {
			b.WriteShort(v)
		}
	}
}

func (b *Buffer) WriteBytes(v Bytes) {
	if b.err == nil {
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
	if b.err == nil {
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
	if b.err == nil {
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
	if b.err == nil {
		// Writes length of the string.
		b.WriteShort(Short(len(s)))
		_, _ = b.buf.WriteString(s)
	}
}

func (b *Buffer) WriteLongString(s string) {
	if b.err == nil {
		// Writes length of the long string.
		b.WriteInt(Int(len(s)))
		_, _ = b.buf.WriteString(s)
	}
}

func (b *Buffer) WriteStringList(l StringList) {
	// Writes length of the string list.
	b.WriteShort(Short(len(l)))
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

	for k, v := range m {
		// Writes key.
		b.WriteString(k)
		// Writes value.
		b.WriteStringList(v)
	}
}

func (b *Buffer) WriteEventTypes(e []EventType) {
	if b.err == nil {
		for _, k := range e {
			if _, ok := ValidEventTypes[k]; !ok {
				b.RecordError(fmt.Errorf("invalid EventType %s", k))
				return
			}
		}
		b.WriteStringList(e)
	}
}

func (b *Buffer) WriteQueryOptions(q QueryOptions) {
	if b.err == nil {
		b.WriteFlags(q.Flags)
		// Checks the flags and writes Values correspondent to the ones that are set.
		if Values&q.Flags != 0 {
			// Writes amount of Values.
			b.WriteShort(Short(len(q.Values)))
			for i := range q.Names {
				if WithNamesForValues&q.Flags != 0 {
					b.WriteString(q.Names[i])
				}
				b.WriteValue(q.Values[i])
			}
		}
		if PageSize&q.Flags != 0 {
			b.WriteInt(q.PageSize)
		}
		if WithPagingState&q.Flags != 0 {
			b.WriteBytes(q.PagingState)
		}
		if WithSerialConsistency&q.Flags != 0 {
			b.WriteConsistency(q.SerialConsistency)
		}
		if WithDefaultTimestamp&q.Flags != 0 {
			b.WriteLong(q.Timestamp)
		}
	}
}

func (b *Buffer) Read(n int) Bytes {
	if b.err == nil {
		p := make(Bytes, n)
		l, err := b.buf.Read(p)
		if l != n || err != nil {
			b.RecordError(fmt.Errorf("invalid Buffer Read"))
		}
		return p
	}
	return nil
}

func (b *Buffer) ReadRawBytes(n int) Bytes {
	if b.err == nil {
		p := b.buf.Next(n)
		if len(p) == n {
			return p
		} else {
			b.RecordError(fmt.Errorf("invalid Buffer ReadRawBytes"))
		}
	}
	return nil
}

func (b *Buffer) ReadByte() Byte {
	if b.err == nil {
		var n Byte
		n, b.err = b.buf.ReadByte()
		return n
	}
	return Byte(0)
}

func (b *Buffer) ReadShort() Short {
	if b.err == nil {
		return Short(b.ReadByte())<<8 | Short(b.ReadByte())
	}
	return Short(0)
}

func (b *Buffer) ReadInt() Int {
	if b.err == nil {
		tmp := [4]byte{0, 0, 0, 0}
		_, b.err = b.buf.Read(tmp[:])
		return Int(tmp[0])<<24 |
			Int(tmp[1])<<16 |
			Int(tmp[2])<<8 |
			Int(tmp[3])
	}
	return Int(0)
}

func (b *Buffer) ReadLong() Long {
	if b.err == nil {
		tmp := [8]byte{0, 0, 0, 0, 0, 0, 0, 0}
		_, b.err = b.buf.Read(tmp[:])
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
	if b.err == nil {
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
	if b.err == nil {
		// Read length of the Bytes.
		n := b.ReadShort()
		return b.Read(int(n))
	}
	return nil
}

// Length equal to -1 represents null.
// Length equal to -2 represents not set.
func (b *Buffer) ReadValue() Value {
	if b.err == nil {
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
	if b.err == nil {
		var n Byte
		// Checks for valid length of the IP address.
		if n, b.err = b.buf.ReadByte(); n == 4 || n == 16 {
			return Inet{IP: b.Read(int(n)), Port: b.ReadInt()}
		} else {
			b.RecordError(fmt.Errorf("invalid ip length"))
		}
	}
	return Inet{}
}

func (b *Buffer) ReadString() string {
	if b.err == nil {
		return string(b.Read(int(b.ReadShort())))
	}
	return ""
}

func (b *Buffer) ReadLongString() string {
	if b.err == nil {
		return string(b.Read(int(b.ReadInt())))
	}
	return ""
}

func (b *Buffer) ReadStringList() StringList {
	if b.err == nil {
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
	if b.err == nil {
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
	if b.err == nil {
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
	if b.err == nil {
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

func (b *Buffer) ParseConsistency() Consistency {
	c := Consistency(b.ReadShort())
	if c > ConsistencyRange {
		b.RecordError(fmt.Errorf("invalid SchemaChangeType: %d", c))
	}
	return c
}

func (b *Buffer) ParseErrorCode() ErrorCode {
	e := ErrorCode(b.ReadInt())
	if _, ok := errorCodes[e]; !ok {
		b.RecordError(fmt.Errorf("invalid error code: %d", e))
	}
	return e
}

func (b *Buffer) ParseWriteType() WriteType {
	w := WriteType(b.ReadString())
	if _, ok := ValidWriteTypes[w]; !ok {
		b.RecordError(fmt.Errorf("invalid write type: %s", w))
	}
	return w
}
