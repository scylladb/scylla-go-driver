package frame

// Generic types from CQL binary protocol.
// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L214-L266
type (
	Int            = int32
	Long           = int64
	Short          = uint16
	Byte           = byte
	UUID           = [16]byte
	StringList     = []string
	Bytes          = []byte
	ShortBytes     = []byte
	StringMap      = map[string]string
	StringMultiMap = map[string][]string
	BytesMap       = map[string]Bytes
)

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L229-L233
type Value struct {
	N     Int
	Bytes Bytes
}

const (
	LengthOfValueNotSet  Int = -2
	LengthOfNullValue    Int = -1
	LengthOfInvalidValue Int = 0
)

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L241-L245
type Inet struct {
	IP   Bytes
	Port Int
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L183-L201
type OpCode = Byte

const (
	OpError         OpCode = 0x00
	OpStartup       OpCode = 0x01
	OpReady         OpCode = 0x02
	OpAuthenticate  OpCode = 0x03
	OpOptions       OpCode = 0x05
	OpSupported     OpCode = 0x06
	OpQuery         OpCode = 0x07
	OpResult        OpCode = 0x08
	OpPrepare       OpCode = 0x09
	OpExecute       OpCode = 0x0A
	OpRegister      OpCode = 0x0B
	OpEvent         OpCode = 0x0C
	OpBatch         OpCode = 0x0D
	OpAuthChallenge OpCode = 0x0E
	OpAuthResponse  OpCode = 0x0F
	OpAuthSuccess   OpCode = 0x10
)

var ValidOpCodes = map[OpCode]struct{}{
	OpError:         {},
	OpStartup:       {},
	OpReady:         {},
	OpAuthenticate:  {},
	OpOptions:       {},
	OpSupported:     {},
	OpQuery:         {},
	OpResult:        {},
	OpPrepare:       {},
	OpExecute:       {},
	OpRegister:      {},
	OpEvent:         {},
	OpBatch:         {},
	OpAuthChallenge: {},
	OpAuthResponse:  {},
	OpAuthSuccess:   {},
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L246-L259
type Consistency = Short

const (
	ANY         Consistency = 0x0000
	ONE         Consistency = 0x0001
	TWO         Consistency = 0x0002
	THREE       Consistency = 0x0003
	QUORUM      Consistency = 0x0004
	ALL         Consistency = 0x0005
	LOCALQUORUM Consistency = 0x0006
	EACHQUORUM  Consistency = 0x0007
	SERIAL      Consistency = 0x0008
	LOCALSERIAL Consistency = 0x0009
	LOCALONE    Consistency = 0x000A
)

const InvalidConsistency Consistency = 0x000B

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L125-L158
type HeaderFlags = Byte

const (
	Compression   HeaderFlags = 0x01
	Tracing       HeaderFlags = 0x02
	CustomPayload HeaderFlags = 0x04
	Warning       HeaderFlags = 0x08
)

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L346-L385
type QueryFlags = Byte

const (
	Values                QueryFlags = 0x01
	SkipMetadata          QueryFlags = 0x02
	PageSize              QueryFlags = 0x04
	WithPagingState       QueryFlags = 0x08
	WithSerialConsistency QueryFlags = 0x10
	WithDefaultTimestamp  QueryFlags = 0x20
	WithNamesForValues    QueryFlags = 0x40
)

type (
	// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L576-L594
	ResultFlags = Int

	// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L684-L690
	PreparedFlags = Int
)

const (
	GlobalTablesSpec ResultFlags = 0x0001
	HasMorePages     ResultFlags = 0x0002
	NoMetadata       ResultFlags = 0x0004
)

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L421-L426
type BatchTypeFlag = Byte

const (
	LoggedBatchFlag   BatchTypeFlag = 0
	UnloggedBatchFlag BatchTypeFlag = 1
	CounterBatchFlag  BatchTypeFlag = 2
)

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L456
type BatchQueryKind = Byte

// CQLv4 is the only protocol version currently supported.
const CQLv4 Byte = 0x84

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1086-L1107
type WriteType string

const (
	Simple        WriteType = "SIMPLE"
	Batch         WriteType = "BATCH"
	UnloggedBatch WriteType = "UNLOGGED_BATCH"
	Counter       WriteType = "COUNTER"
	BatchLog      WriteType = "BATCH_LOG"
	CAS           WriteType = "CAS"
	View          WriteType = "VIEW"
	CDC           WriteType = "CDC"
)

var ValidWriteTypes = map[WriteType]struct{}{
	Simple:        {},
	Batch:         {},
	UnloggedBatch: {},
	Counter:       {},
	BatchLog:      {},
	CAS:           {},
	View:          {},
	CDC:           {},
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L757-L791
type EventType = string

const (
	TopologyChange EventType = "TOPOLOGY_CHANGE"
	StatusChange   EventType = "STATUS_CHANGE"
	SchemaChange   EventType = "SCHEMA_CHANGE"
)

var ValidEventTypes = map[EventType]struct{}{
	TopologyChange: {},
	StatusChange:   {},
	SchemaChange:   {},
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L760-L765
type TopologyChangeType string

const (
	NewNode     TopologyChangeType = "NEW_NODE"
	RemovedNode TopologyChangeType = "REMOVED_NODE"
)

var topologyChangeTypes = map[TopologyChangeType]struct{}{
	NewNode:     {},
	RemovedNode: {},
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L766-L770
type StatusChangeType string

const (
	Up   StatusChangeType = "UP"
	Down StatusChangeType = "DOWN"
)

var statusChangeTypes = map[StatusChangeType]struct{}{
	Up:   {},
	Down: {},
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L771-L791
type SchemaChangeType string

const (
	Created SchemaChangeType = "CREATED"
	Updated SchemaChangeType = "UPDATED"
	Dropped SchemaChangeType = "DROPPED"
)

var schemaChangeTypes = map[SchemaChangeType]struct{}{
	Created: {},
	Updated: {},
	Dropped: {},
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L775-L779
type SchemaChangeTarget string

const (
	Keyspace  SchemaChangeTarget = "KEYSPACE"
	Table     SchemaChangeTarget = "TABLE"
	UserType  SchemaChangeTarget = "TYPE"
	Function  SchemaChangeTarget = "FUNCTION"
	Aggregate SchemaChangeTarget = "AGGREGATE"
)

var validSchemaChangeTargets = map[SchemaChangeTarget]struct{}{
	Keyspace:  {},
	Table:     {},
	UserType:  {},
	Function:  {},
	Aggregate: {},
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L296-L308
type StartupOptions StringMap

// Mandatory values and keys that can be given in Startup body
// value in the map means option name and key means its possible values.
var mandatoryOptions = StringMultiMap{
	"CQL_VERSION": {"3.0.0"},
}

var possibleOptions = StringMultiMap{
	"COMPRESSION": {
		"lz4",
		"snappy",
	},
	"NO_COMPACT":        {},
	"THROW_ON_OVERLOAD": {},
}

// QueryOptions represent optional Values defined by flags.
// Consists of Values required for all flags.
// Values for unset flags are uninitialized.
// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L350-L385
type QueryOptions struct {
	Flags             QueryFlags
	Values            []Value
	Names             StringList
	PageSize          Int
	PagingState       Bytes
	SerialConsistency Consistency
	Timestamp         Long
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L236-L239
type OptionID Short

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L615-L658
const (
	CustomID    OptionID = 0x0000
	ASCIIID     OptionID = 0x0001
	BigintID    OptionID = 0x0002
	BlobID      OptionID = 0x0003
	BooleanID   OptionID = 0x0004
	CounterID   OptionID = 0x0005
	DecimalID   OptionID = 0x0006
	DoubleID    OptionID = 0x0007
	FloatID     OptionID = 0x0008
	IntID       OptionID = 0x0009
	TimestampID OptionID = 0x000B
	UUIDID      OptionID = 0x000C
	VarcharID   OptionID = 0x000D
	VarintID    OptionID = 0x000E
	TimeUUIDID  OptionID = 0x000F
	InetID      OptionID = 0x0010
	DateID      OptionID = 0x0011
	TimeID      OptionID = 0x0012
	SmallintID  OptionID = 0x0013
	TinyintID   OptionID = 0x0014
	ListID      OptionID = 0x0020
	MapID       OptionID = 0x0021
	SetID       OptionID = 0x0022
	UDTID       OptionID = 0x0030
	TupleID     OptionID = 0x0031
)

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L612-L617
type CustomOption struct {
	Name string
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L637-L638
type ListOption struct {
	Element Option
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L639-L640
type MapOption struct {
	Key   Option
	Value Option
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L641-L642
type SetOption struct {
	Element Option
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L643-L654
type UDTOption struct {
	Keyspace   string
	Name       string
	fieldNames []string
	fieldTypes []Option
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L655-L658
type TupleOption struct {
	ValueTypes []Option
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L236-L239
type Option struct {
	ID     OptionID
	Custom *CustomOption
	List   *ListOption
	Map    *MapOption
	Set    *SetOption
	UDT    *UDTOption
	Tuple  *TupleOption
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L240
type OptionList []Option

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L573-L658
type ResultMetadata struct {
	Flags      ResultFlags
	ColumnsCnt Int

	// nil if flagPagingState is not set
	PagingState    Bytes
	GlobalKeyspace string
	GlobalTable    string

	Columns []ColumnSpec
}

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L601-L658
type ColumnSpec struct {
	Keyspace string
	Table    string
	Name     string
	Type     Option
}

type Row []Bytes

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L681-L724
type PreparedMetadata struct {
	Flags          PreparedFlags
	ColumnsCnt     Int
	PkCnt          Int
	PkIndexes      []Short
	GlobalKeyspace string
	GlobalTable    string
	Columns        []ColumnSpec
}
