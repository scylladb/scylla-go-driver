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

func (b *Buffer) recordError(err error) {
	if b.err != nil {
		return
	}

	b.err = err
}

func (b *Buffer) Write(v Bytes) {
	if b.err != nil {
		return
	}

	_, _ = b.buf.Write(v)
}

func (b *Buffer) WriteByte(v Byte) {
	if b.err != nil {
		return
	}

	_ = b.buf.WriteByte(v)
}

func (b *Buffer) WriteShort(v Short) {
	if b.err != nil {
		return
	}

	_, _ = b.buf.Write([]byte{
		byte(v >> 8),
		byte(v),
	})
}

func (b *Buffer) WriteInt(v Int) {
	if b.err != nil {
		return
	}

	_, _ = b.buf.Write([]byte{
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	})
}

func (b *Buffer) WriteLong(v Long) {
	if b.err != nil {
		return
	}

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
	if b.err != nil {
		return
	}

	b.WriteByte(v)
}

func (b *Buffer) WriteHeaderFlags(v HeaderFlags) {
	if b.err != nil {
		return
	}

	b.WriteByte(v)
}

func (b *Buffer) WriteQueryFlags(v QueryFlags) {
	if b.err != nil {
		return
	}

	b.WriteByte(v)
}

func (b *Buffer) WriteResultFlags(v ResultFlags) {
	if b.err != nil {
		return
	}

	b.WriteInt(v)
}

func (b *Buffer) WritePreparedFlags(v PreparedFlags) {
	if b.err != nil {
		return
	}

	b.WriteInt(v)
}

func (b *Buffer) WriteOpCode(v OpCode) {
	if b.err != nil {
		return
	}

	if _, ok := ValidOpCodes[v]; ok {
		b.WriteByte(v)
	} else {
		b.recordError(fmt.Errorf("invalid operation code: %v", v))
	}
}

func (b *Buffer) WriteUUID(v UUID) {
	if b.err != nil {
		return
	}

	if len(v) != 16 {
		b.recordError(fmt.Errorf("UUID has invalid length: %d", len(v)))
	} else {
		b.Write(v)
	}
}

func (b *Buffer) WriteConsistency(v Consistency) {
	if b.err != nil {
		return
	}

	if v >= InvalidConsistency {
		b.recordError(fmt.Errorf("invalid consistency: %v", v))
	} else {
		b.WriteShort(v)
	}
}

func (b *Buffer) WriteBytes(v Bytes) {
	if b.err != nil {
		return
	}

	if v == nil {
		b.WriteInt(-1)
	} else {
		// Writes length of the bytes.
		b.WriteInt(Int(len(v)))
		b.Write(v)
	}
}

func (b *Buffer) WriteShortBytes(v Bytes) {
	if b.err != nil {
		return
	}

	// WriteTo length of the bytes.
	b.WriteShort(Short(len(v)))
	b.Write(v)
}

func (b *Buffer) WriteValue(v Value) {
	if b.err != nil {
		return
	}

	b.WriteInt(v.N)
	// Writes value's body if there is any.
	if v.N > 0 {
		_, _ = b.buf.Write(v.Bytes)
	} else if v.N < -2 {
		b.recordError(fmt.Errorf("invalid value"))
	}
}

func (b *Buffer) WriteInet(v Inet) {
	if b.err != nil {
		return
	}

	if len(v.IP) != 4 && len(v.IP) != 16 {
		b.recordError(fmt.Errorf("invalid IP length"))
	} else {
		// Writes length of the IP address.
		b.WriteByte(Byte(len(v.IP)))
		b.Write(v.IP)
		b.WriteInt(v.Port)
	}
}

func (b *Buffer) WriteString(s string) {
	if b.err != nil {
		return
	}

	// Writes length of the string.
	b.WriteShort(Short(len(s)))
	_, _ = b.buf.WriteString(s)
}

func (b *Buffer) WriteLongString(s string) {
	if b.err != nil {
		return
	}

	// Writes length of the long string.
	b.WriteInt(Int(len(s)))
	_, _ = b.buf.WriteString(s)
}

func (b *Buffer) WriteStringList(l StringList) {
	if b.err != nil {
		return
	}

	// Writes length of the string list.
	b.WriteShort(Short(len(l)))
	for _, s := range l {
		b.WriteString(s)
	}
}

func (b *Buffer) WriteStringMap(m StringMap) {
	if b.err != nil {
		return
	}

	// Writes the number of elements in the map.
	b.WriteShort(Short(len(m)))
	for k, v := range m {
		b.WriteString(k)
		b.WriteString(v)
	}
}

func (b *Buffer) WriteStringMultiMap(m StringMultiMap) {
	if b.err != nil {
		return
	}

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
	if b.err != nil {
		return
	}

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
	if b.err != nil {
		return
	}

	for _, k := range e {
		if _, ok := ValidEventTypes[k]; !ok {
			b.recordError(fmt.Errorf("invalid EventType %s", k))
			return
		}
	}
	b.WriteStringList(e)
}

func (b *Buffer) WriteQueryOptions(q QueryOptions) { // nolint:gocritic
	if b.err != nil {
		return
	}

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

func (b *Buffer) Read(n int) Bytes {
	if b.err != nil {
		return nil
	}

	p := make(Bytes, n)
	l, err := b.buf.Read(p)
	if l != n || err != nil {
		b.recordError(fmt.Errorf("invalid Buffer Read"))
	}
	return p
}

func (b *Buffer) ReadRawBytes(n int) Bytes {
	if b.err != nil {
		return nil
	}

	p := b.buf.Next(n)
	if len(p) == n {
		return p
	}
	b.recordError(fmt.Errorf("invalid Buffer ReadRawBytes"))

	return nil
}

func (b *Buffer) ReadByte() Byte {
	if b.err != nil {
		return Byte(0)
	}

	var n Byte
	n, b.err = b.buf.ReadByte()
	return n
}

func (b *Buffer) ReadShort() Short {
	if b.err != nil {
		return Short(0)
	}

	return Short(b.ReadByte())<<8 | Short(b.ReadByte())
}

func (b *Buffer) ReadInt() Int {
	if b.err != nil {
		return Int(0)
	}

	tmp := [4]byte{0, 0, 0, 0}
	_, b.err = b.buf.Read(tmp[:])
	return Int(tmp[0])<<24 |
		Int(tmp[1])<<16 |
		Int(tmp[2])<<8 |
		Int(tmp[3])
}

func (b *Buffer) ReadLong() Long {
	if b.err != nil {
		return Long(0)
	}

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

func (b *Buffer) ReadOpCode() OpCode {
	if b.err != nil {
		return OpCode(0)
	}

	o := b.ReadByte()
	// OpAuthSuccess holds the biggest number among operation codes.
	if o > OpAuthSuccess {
		b.recordError(fmt.Errorf("invalid operation code: %v", o))
	}
	return o
}

func (b *Buffer) ReadUUID() UUID {
	if b.err != nil {
		return UUID{0}
	}

	u := b.Read(16)
	if len(u) != 16 {
		b.recordError(fmt.Errorf("UUID has invalid length: %d", len(u)))
	}
	return u
}

func (b *Buffer) ReadHeaderFlags() QueryFlags {
	if b.err != nil {
		return QueryFlags(0)
	}

	return b.ReadByte()
}

func (b *Buffer) ReadQueryFlags() QueryFlags {
	if b.err != nil {
		return QueryFlags(0)
	}

	return b.ReadByte()
}

func (b *Buffer) ReadResultFlags() ResultFlags {
	if b.err != nil {
		return ResultFlags(0)
	}

	return b.ReadInt()
}

func (b *Buffer) ReadPreparedFlags() PreparedFlags {
	if b.err != nil {
		return PreparedFlags(0)
	}

	return b.ReadInt()
}

func (b *Buffer) ReadBytes() Bytes {
	if b.err != nil {
		return nil
	}

	n := b.ReadInt()
	if n < 0 {
		return nil
	}
	return b.Read(int(n))
}

func (b *Buffer) ReadShortBytes() ShortBytes {
	if b.err != nil {
		return nil
	}

	return b.Read(int(b.ReadShort()))
}

// Length equal to -1 represents null.
// Length equal to -2 represents not set.
func (b *Buffer) ReadValue() Value {
	if b.err != nil {
		return Value{}
	}

	n := b.ReadInt()
	if n < -2 {
		b.recordError(fmt.Errorf("invalid value length"))
	} else if n > 0 {
		return Value{N: n, Bytes: b.Read(int(n))}
	}

	return Value{N: n}
}

func (b *Buffer) ReadInet() Inet {
	if b.err != nil {
		return Inet{}
	}

	var n Byte
	// Checks for valid length of the IP address.
	if n, b.err = b.buf.ReadByte(); n == 4 || n == 16 {
		return Inet{IP: b.Read(int(n)), Port: b.ReadInt()}
	}

	b.recordError(fmt.Errorf("invalid ip length"))
	return Inet{}
}

func (b *Buffer) ReadString() string {
	if b.err != nil {
		return ""
	}

	return string(b.Read(int(b.ReadShort())))
}

func (b *Buffer) ReadLongString() string {
	if b.err != nil {
		return ""
	}

	return string(b.Read(int(b.ReadInt())))
}

func (b *Buffer) ReadStringList() StringList {
	if b.err != nil {
		return nil
	}

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
	if b.err != nil {
		return nil
	}

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
	if b.err != nil {
		return nil
	}

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
	if b.err != nil {
		return nil
	}

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
	if b.err != nil {
		return nil
	}

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
	if b.err != nil {
		return
	}

	count := 0
	for k, v := range mandatoryOptions {
		if s, ok := m[k]; !(ok && contains(v, s)) {
			b.recordError(fmt.Errorf("invalid mandatory Startup option %s: %s", k, s))
			return
		}
		count++
	}
	for k, v := range possibleOptions {
		if s, ok := m[k]; ok && !contains(v, s) {
			b.recordError(fmt.Errorf("invalid Startup option %s: %s", k, s))
			return
		} else if ok {
			count++
		}
	}
	if count != len(m) {
		b.recordError(fmt.Errorf("invalid Startup option"))
		return
	}

	b.WriteStringMap(m)
}

func (b *Buffer) ReadTopologyChangeType() TopologyChangeType {
	if b.err != nil {
		return ""
	}

	t := TopologyChangeType(b.ReadString())
	if _, ok := topologyChangeTypes[t]; !ok {
		b.recordError(fmt.Errorf("invalid TopologyChangeType: %s", t))
	}
	return t
}

func (b *Buffer) ReadStatusChangeType() StatusChangeType {
	if b.err != nil {
		return ""
	}

	t := StatusChangeType(b.ReadString())
	if _, ok := statusChangeTypes[t]; !ok {
		b.recordError(fmt.Errorf("invalid StatusChangeType: %s", t))
	}
	return t
}

func (b *Buffer) ReadSchemaChangeType() SchemaChangeType {
	if b.err != nil {
		return ""
	}

	t := SchemaChangeType(b.ReadString())
	if _, ok := schemaChangeTypes[t]; !ok {
		b.recordError(fmt.Errorf("invalid SchemaChangeType: %s", t))
	}
	return t
}

// Validation is not required. It is done inside SchemaChange event.
func (b *Buffer) ReadSchemaChangeTarget() SchemaChangeTarget {
	if b.err != nil {
		return ""
	}

	v := SchemaChangeTarget(b.ReadString())
	if _, ok := validSchemaChangeTargets[v]; !ok {
		b.recordError(fmt.Errorf("invalid SchemaChangeTarget: %s", v))
	}
	return v
}

func (b *Buffer) ReadErrorCode() ErrorCode {
	if b.err != nil {
		return ErrorCode(0)
	}

	v := b.ReadInt()
	if _, ok := validErrorCodes[v]; !ok {
		b.recordError(fmt.Errorf("invalid error code: %d", v))
	}
	return v
}

func (b *Buffer) ReadConsistency() Consistency {
	if b.err != nil {
		return Consistency(0)
	}

	v := b.ReadShort()
	if v >= InvalidConsistency {
		b.recordError(fmt.Errorf("invalid consistency: %v", v))
	}
	return v
}

func (b *Buffer) ReadWriteType() WriteType {
	if b.err != nil {
		return ""
	}

	w := WriteType(b.ReadString())
	if _, ok := ValidWriteTypes[w]; !ok {
		b.recordError(fmt.Errorf("invalid write type: %s", w))
	}
	return w
}

func (b *Buffer) ReadCustomOption() *CustomOption {
	if b.err != nil {
		return nil
	}

	return &CustomOption{
		Name: b.ReadString(),
	}
}

func (b *Buffer) ReadListOption() *ListOption {
	if b.err != nil {
		return nil
	}

	return &ListOption{
		Element: b.ReadOption(),
	}
}

func (b *Buffer) ReadMapOption() *MapOption {
	if b.err != nil {
		return nil
	}

	return &MapOption{
		Key:   b.ReadOption(),
		Value: b.ReadOption(),
	}
}

func (b *Buffer) ReadSetOption() *SetOption {
	if b.err != nil {
		return nil
	}

	return &SetOption{
		Element: b.ReadOption(),
	}
}

func (b *Buffer) ReadUDTOption() *UDTOption {
	if b.err != nil {
		return nil
	}

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
	if b.err != nil {
		return nil
	}

	return &TupleOption{
		ValueTypes: b.ReadOptionList(),
	}
}

func (b *Buffer) ReadOptionList() OptionList {
	if b.err != nil {
		return nil
	}

	n := b.ReadShort()
	ol := make(OptionList, n)
	for i := range ol {
		ol[i] = b.ReadOption()
	}
	return ol
}

func (b *Buffer) ReadOption() Option {
	if b.err != nil {
		return Option{}
	}

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
		if id < ASCIIID || TinyintID < id {
			b.recordError(fmt.Errorf("invalid Option ID: %d", id))
		}
		return Option{
			ID: id,
		}
	}
}

func (b *Buffer) ReadRow(n Int) Row {
	if b.err != nil {
		return nil
	}

	r := make([]Bytes, n)
	for i := range r {
		r[i] = b.ReadBytes()
	}
	return r
}

func (b *Buffer) ReadColumnSpec(f ResultFlags) ColumnSpec {
	if b.err != nil {
		return ColumnSpec{}
	}

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
	if b.err != nil {
		return ResultMetadata{}
	}

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
	if b.err != nil {
		return PreparedMetadata{}
	}

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
