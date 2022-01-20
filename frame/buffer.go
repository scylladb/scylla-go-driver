package frame

import (
	"bytes"
	"fmt"
	"log"
)

var Debug = true

type Buffer struct {
	buf bytes.Buffer
}

func (b *Buffer) Bytes() []byte {
	return b.buf.Bytes()
}

func (b *Buffer) Reset() {
	b.buf.Reset()
}

func (b *Buffer) Write(v Bytes) {
	_, _ = b.buf.Write(v)
}

func (b *Buffer) WriteByte(v Byte) {
	_ = b.buf.WriteByte(v)
}

func (b *Buffer) WriteShort(v Short) {
	_, _ = b.buf.Write([]byte{
		byte(v >> 8),
		byte(v),
	})
}

func (b *Buffer) WriteInt(v Int) {
	_, _ = b.buf.Write([]byte{
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	})
}

func (b *Buffer) WriteLong(v Long) {
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

func (b *Buffer) WriteBatchTypeFlag(v BatchTypeFlag) {
	b.WriteByte(v)
}

func (b *Buffer) WriteHeaderFlags(v HeaderFlags) {
	b.WriteByte(v)
}

func (b *Buffer) WriteQueryFlags(v QueryFlags) {
	b.WriteByte(v)
}

func (b *Buffer) WriteResultFlags(v ResultFlags) {
	b.WriteInt(v)
}

func (b *Buffer) WritePreparedFlags(v PreparedFlags) {
	b.WriteInt(v)
}

func (b *Buffer) WriteOpCode(v OpCode) {
	if Debug {
		if _, ok := allOpCodes[v]; !ok {
			log.Printf("unknown operation code: %v", v)
		}
	}

	b.WriteByte(v)
}

func (b *Buffer) WriteUUID(v UUID) {
	b.Write(v[:])
}

func (b *Buffer) WriteConsistency(v Consistency) {
	if Debug {
		if v > LOCALONE {
			log.Printf("unknown consistency: %v", v)
		}
	}

	b.WriteShort(v)
}

func (b *Buffer) WriteBytes(v Bytes) {
	if v == nil {
		b.WriteInt(-1)
	} else {
		// Writes length of the bytes.
		b.WriteInt(Int(len(v)))
		b.Write(v)
	}
}

func (b *Buffer) WriteShortBytes(v Bytes) {
	// WriteTo length of the bytes.
	b.WriteShort(Short(len(v)))
	b.Write(v)
}

func (b *Buffer) WriteValue(v Value) {
	b.WriteInt(v.N)
	if Debug {
		if v.N < -2 {
			log.Printf("unsupported value length")
		}
	}

	if v.N > 0 {
		_, _ = b.buf.Write(v.Bytes)
	}
}

func (b *Buffer) WriteInet(v Inet) {
	if Debug {
		if l := len(v.IP); l != 4 && l != 16 {
			log.Printf("unknown IP length")
		}
	}

	b.WriteByte(Byte(len(v.IP)))
	b.Write(v.IP)
	b.WriteInt(v.Port)
}

func (b *Buffer) WriteString(s string) {
	// Writes length of the string.
	b.WriteShort(Short(len(s)))
	_, _ = b.buf.WriteString(s)
}

func (b *Buffer) WriteLongString(s string) {
	// Writes length of the long string.
	b.WriteInt(Int(len(s)))
	_, _ = b.buf.WriteString(s)
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

func (b *Buffer) WriteBytesMap(m BytesMap) {
	// Writes the number of elements in the map.
	b.WriteShort(Short(len(m)))
	for k, v := range m {
		// Writes key.
		b.WriteString(k)
		// Writes value.
		b.WriteBytes(v)
	}
}

func (b *Buffer) WriteEventTypes(e []EventType) {
	if Debug {
		for _, k := range e {
			if _, ok := allEventTypes[k]; !ok {
				log.Printf("unknown EventType %s", k)
			}
		}
	}
	b.WriteStringList(e)
}

func (b *Buffer) WriteQueryOptions(q QueryOptions) { // nolint:gocritic
	b.WriteQueryFlags(q.Flags)
	// Checks the flags and writes Values correspondent to the ones that are set.
	if Values&q.Flags != 0 {
		// Writes amount of Values.
		b.WriteShort(Short(len(q.Values)))
		for i := range q.Values {
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

// read method reads n next bytes from the buffer by making a copy.
func (b *Buffer) read(n int) Bytes {
	p := make(Bytes, n)
	l, err := b.buf.Read(p)
	if err != nil {
		panic(fmt.Errorf("buffer read error: %w", err))
	}
	if l != n {
		panic(fmt.Errorf("buffer read error: invalid length"))
	}
	return p
}

func (b *Buffer) ReadByte() Byte {
	var n Byte
	var err error
	if n, err = b.buf.ReadByte(); err != nil {
		panic(fmt.Errorf("buffer readByte error: %w", err))
	}
	return n
}

func (b *Buffer) ReadShort() Short {
	return Short(b.ReadByte())<<8 | Short(b.ReadByte())
}

func (b *Buffer) ReadInt() Int {
	tmp := [4]byte{0, 0, 0, 0}
	_, err := b.buf.Read(tmp[:])
	if err != nil {
		panic(fmt.Errorf("buffer readInt error: %w", err))
	}
	return Int(tmp[0])<<24 |
		Int(tmp[1])<<16 |
		Int(tmp[2])<<8 |
		Int(tmp[3])
}

func (b *Buffer) ReadLong() Long {
	tmp := [8]byte{0, 0, 0, 0, 0, 0, 0, 0}
	_, err := b.buf.Read(tmp[:])
	if err != nil {
		panic(fmt.Errorf("buffer readLong error: %w", err))
	}
	return Long(tmp[0])<<56 |
		Long(tmp[1])<<48 |
		Long(tmp[2])<<40 |
		Long(tmp[3])<<32 |
		Long(tmp[4])<<24 |
		Long(tmp[5])<<16 |
		Long(tmp[6])<<8 |
		Long(tmp[7])
}

func (b *Buffer) ReadOpCode() OpCode {
	o := b.ReadByte()
	if Debug {
		if o > OpAuthSuccess {
			log.Printf("unknown operation code: %v", o)
		}
	}
	return o
}

func (b *Buffer) ReadUUID() UUID {
	var u UUID
	if _, err := b.buf.Read(u[:]); err != nil {
		panic(fmt.Errorf("buffer readUUID error: %w", err))
	}
	return u
}

func (b *Buffer) ReadHeaderFlags() QueryFlags {
	return b.ReadByte()
}

func (b *Buffer) ReadQueryFlags() QueryFlags {
	return b.ReadByte()
}

func (b *Buffer) ReadResultFlags() ResultFlags {
	return b.ReadInt()
}

func (b *Buffer) ReadPreparedFlags() PreparedFlags {
	return b.ReadInt()
}

func (b *Buffer) ReadBytes() Bytes {
	n := b.ReadInt()
	if n < 0 {
		return nil
	}
	return b.read(int(n))
}

func (b *Buffer) ReadShortBytes() ShortBytes {
	return b.read(int(b.ReadShort()))
}

// Length equal to -1 represents null.
// Length equal to -2 represents not set.
func (b *Buffer) ReadValue() Value {
	n := b.ReadInt()
	if Debug {
		if n < -2 {
			log.Printf("unknown value length")
		}
	}

	v := Value{N: n}
	if n > 0 {
		v.Bytes = b.read(int(n))
	}
	return v
}

func (b *Buffer) ReadInet() Inet {
	var (
		n   Byte
		err error
	)
	if n, err = b.buf.ReadByte(); err != nil {
		panic(err)
	}

	if Debug {
		if n != 4 && n != 16 {
			log.Printf("unknown ip length")
		}
	}

	return Inet{IP: b.read(int(n)), Port: b.ReadInt()}
}

func (b *Buffer) ReadString() string {
	return string(b.read(int(b.ReadShort())))
}

func (b *Buffer) ReadLongString() string {
	return string(b.read(int(b.ReadInt())))
}

func (b *Buffer) ReadStringList() StringList {
	// Read length of the string list.
	n := b.ReadShort()
	l := make(StringList, 0, n)
	for i := Short(0); i < n; i++ {
		// Read the strings and append them to the list.
		l = append(l, b.ReadString())
	}
	return l
}

func (b *Buffer) ReadStringMap() StringMap {
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

func (b *Buffer) ReadStringMultiMap() StringMultiMap {
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

func (b *Buffer) ReadBytesMap() BytesMap {
	n := b.ReadShort()
	m := make(BytesMap, n)
	for i := Short(0); i < n; i++ {
		k := b.ReadString()
		v := b.ReadBytes()
		m[k] = v
	}
	return m
}

func (b *Buffer) ReadStartupOptions() StartupOptions {
	return b.ReadStringMap()
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
	count := 0
	if Debug {
		for k, v := range mandatoryOptions {
			if s, ok := m[k]; !(ok && contains(v, s)) {
				log.Printf("unknown mandatory Startup option %s: %s", k, s)
			}
			count++
		}
		for k, v := range possibleOptions {
			if s, ok := m[k]; ok && !contains(v, s) {
				log.Printf("unknown Startup option %s: %s", k, s)
			} else if ok {
				count++
			}
		}
		if count != len(m) {
			log.Printf("unknown Startup option")
		}
	}

	b.WriteStringMap(m)
}

func (b *Buffer) ReadTopologyChangeType() TopologyChangeType {
	t := TopologyChangeType(b.ReadString())
	if Debug {
		if _, ok := allTopologyChangeTypes[t]; !ok {
			log.Printf("unknown TopologyChangeType: %s", t)
		}
	}
	return t
}

func (b *Buffer) ReadStatusChangeType() StatusChangeType {
	t := StatusChangeType(b.ReadString())
	if Debug {
		if _, ok := allStatusChangeTypes[t]; !ok {
			log.Printf("unknown StatusChangeType: %s", t)
		}
	}
	return t
}

func (b *Buffer) ReadSchemaChangeType() SchemaChangeType {
	t := SchemaChangeType(b.ReadString())
	if Debug {
		if _, ok := allSchemaChangeTypes[t]; !ok {
			log.Printf("unknown SchemaChangeType: %s", t)
		}
	}
	return t
}

// allation is not required. It is done inside SchemaChange event.
func (b *Buffer) ReadSchemaChangeTarget() SchemaChangeTarget {
	v := SchemaChangeTarget(b.ReadString())
	if Debug {
		if _, ok := allSchemaChangeTargets[v]; !ok {
			log.Printf("unknown SchemaChangeTarget: %s", v)
		}
	}
	return v
}

func (b *Buffer) ReadErrorCode() ErrorCode {
	v := b.ReadInt()
	if Debug {
		if _, ok := allErrorCodes[v]; !ok {
			log.Printf("unknown error code: %d", v)
		}
	}
	return v
}

func (b *Buffer) ReadConsistency() Consistency {
	v := b.ReadShort()
	if Debug && v > LOCALONE {
		log.Printf("unknown consistency: %v", v)
	}
	return v
}

func (b *Buffer) ReadWriteType() WriteType {
	w := WriteType(b.ReadString())
	if Debug {
		if _, ok := allWriteTypes[w]; !ok {
			log.Printf("unknown write type: %s", w)
		}
	}
	return w
}

func (b *Buffer) ReadCustomOption() *CustomOption {
	return &CustomOption{
		Name: b.ReadString(),
	}
}

func (b *Buffer) ReadListOption() *ListOption {
	return &ListOption{
		Element: b.ReadOption(),
	}
}

func (b *Buffer) ReadMapOption() *MapOption {
	return &MapOption{
		Key:   b.ReadOption(),
		Value: b.ReadOption(),
	}
}

func (b *Buffer) ReadSetOption() *SetOption {
	return &SetOption{
		Element: b.ReadOption(),
	}
}

func (b *Buffer) ReadUDTOption() *UDTOption {
	ks := b.ReadString()
	name := b.ReadString()
	n := b.ReadShort()
	fn := make(StringList, n)
	ft := make(OptionList, n)

	for i := range fn {
		fn[i] = b.ReadString()
		ft[i] = b.ReadOption()
	}

	return &UDTOption{
		Keyspace:   ks,
		Name:       name,
		fieldNames: fn,
		fieldTypes: ft,
	}
}

func (b *Buffer) ReadTupleOption() *TupleOption {
	return &TupleOption{
		ValueTypes: b.ReadOptionList(),
	}
}

func (b *Buffer) ReadOptionList() OptionList {
	n := b.ReadShort()
	ol := make(OptionList, n)
	for i := range ol {
		ol[i] = b.ReadOption()
	}
	return ol
}

func (b *Buffer) ReadOption() Option {
	id := OptionID(b.ReadShort())
	switch id {
	case CustomID:
		return Option{
			ID:     id,
			Custom: b.ReadCustomOption(),
		}
	case ListID:
		return Option{
			ID:   id,
			List: b.ReadListOption(),
		}
	case MapID:
		return Option{
			ID:  id,
			Map: b.ReadMapOption(),
		}
	case SetID:
		return Option{
			ID:  id,
			Set: b.ReadSetOption(),
		}
	case UDTID:
		return Option{
			ID:  id,
			UDT: b.ReadUDTOption(),
		}
	case TupleID:
		return Option{
			ID:    id,
			Tuple: b.ReadTupleOption(),
		}
	default:
		if Debug {
			if id < ASCIIID || TinyintID < id {
				log.Printf("unknown Option ID: %d", id)
			}
		}
		return Option{
			ID: id,
		}
	}
}

func (b *Buffer) ReadRow(n Int) Row {
	r := make([]Bytes, n)
	for i := range r {
		r[i] = b.ReadBytes()
	}
	return r
}

func (b *Buffer) ReadColumnSpec(f ResultFlags) ColumnSpec {
	if f&GlobalTablesSpec == 0 {
		return ColumnSpec{
			Keyspace: b.ReadString(),
			Table:    b.ReadString(),
			Name:     b.ReadString(),
			Type:     b.ReadOption(),
		}
	}

	return ColumnSpec{
		Name: b.ReadString(),
		Type: b.ReadOption(),
	}
}

func (b *Buffer) ReadResultMetadata() ResultMetadata {
	r := ResultMetadata{
		Flags:      b.ReadResultFlags(),
		ColumnsCnt: b.ReadInt(),
	}

	if r.Flags&HasMorePages != 0 {
		r.PagingState = b.ReadBytes()
	}

	if r.Flags&NoMetadata != 0 {
		return r
	}

	if r.Flags&GlobalTablesSpec != 0 {
		r.GlobalKeyspace = b.ReadString()
		r.GlobalTable = b.ReadString()
	}

	r.Columns = make([]ColumnSpec, r.ColumnsCnt)
	for i := range r.Columns {
		r.Columns[i] = b.ReadColumnSpec(r.Flags)
	}

	return r
}

func (b *Buffer) ReadPreparedMetadata() PreparedMetadata {
	p := PreparedMetadata{
		Flags:      b.ReadPreparedFlags(),
		ColumnsCnt: b.ReadInt(),
		PkCnt:      b.ReadInt(),
	}

	p.PkIndexes = make([]Short, p.PkCnt)
	for i := range p.PkIndexes {
		p.PkIndexes[i] = b.ReadShort()
	}

	if p.Flags&GlobalTablesSpec != 0 {
		p.GlobalKeyspace = b.ReadString()
		p.GlobalTable = b.ReadString()
	}

	p.Columns = make([]ColumnSpec, p.ColumnsCnt)
	for i := range p.Columns {
		p.Columns[i] = b.ReadColumnSpec(p.Flags)
	}

	return p
}
