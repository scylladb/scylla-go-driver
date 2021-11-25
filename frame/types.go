package frame

import "scylla-go-driver/frame/response"

// Generic types from CQL binary protocol.
// https://github.com/apache/cassandra/blob/951d72cd929d1f6c9329becbdd7604a9e709587b/doc/native_protocol_v4.spec#L214
type (
	Byte           = byte
	Short          = uint16
	Int            = int32
	Long           = int64
	UUID           = [16]byte
	StringList     = []string
	StringMap      = map[string]string
	StringMultiMap = map[string][]string
	Bytes          = []byte
)

type Value struct {
	N     Int
	Bytes Bytes
}

type Inet struct {
	IP   Bytes
	Port Int
}

// https://github.com/apache/cassandra/blob/951d72cd929d1f6c9329becbdd7604a9e709587b/doc/native_protocol_v4.spec#L183
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

// https://github.com/apache/cassandra/blob/951d72cd929d1f6c9329becbdd7604a9e709587b/doc/native_protocol_v4.spec#L246
type Consistency = Short

const (
	ANY          Consistency = 0x0000
	ONE          Consistency = 0x0001
	TWO          Consistency = 0x0002
	THREE        Consistency = 0x0003
	QUORUM       Consistency = 0x0004
	ALL          Consistency = 0x0005
	LOCAL_QUORUM Consistency = 0x0006
	EACH_QUORUM  Consistency = 0x0007
	SERIAL       Consistency = 0x0008
	LOCAL_SERIAL Consistency = 0x0009
	LOCAL_ONE    Consistency = 0x000A
	INVALID      Consistency = 0x000B
)

type Flags = Byte

const (
	Values                Flags = 0x01
	SkipMetadata          Flags = 0x02
	PageSize              Flags = 0x04
	WithPagingState       Flags = 0x08
	WithSerialConsistency Flags = 0x10
	WithDefaultTimestamp  Flags = 0x20
	WithNamesForValues    Flags = 0x40
)

// CQLv4 is the only protocol version currently supported.
const CQLv4 Byte = 0x84

// https://github.com/apache/cassandra/blob/951d72cd929d1f6c9329becbdd7604a9e709587b/doc/native_protocol_v4.spec#L1086
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

var ValidWriteTypes = map[WriteType]bool{
	Simple:        true,
	Batch:         true,
	UnloggedBatch: true,
	Counter:       true,
	BatchLog:      true,
	CAS:           true,
	View:          true,
	CDC:           true,
}

type TopologyChangeType string

const (
	NewNode     TopologyChangeType = "NEW_NODE"
	RemovedNode TopologyChangeType = "REMOVED_NODE"
)

var topologyChangeTypes = map[TopologyChangeType]bool{
	NewNode:     true,
	RemovedNode: true,
}

type StatusChangeType string

const (
	Up   StatusChangeType = "UP"
	Down StatusChangeType = "DOWN"
)

var statusChangeTypes = map[StatusChangeType]bool{
	Up:   true,
	Down: true,
}

type SchemaChangeType string

const (
	Created SchemaChangeType = "CREATED"
	Updated SchemaChangeType = "UPDATED"
	Dropped SchemaChangeType = "DROPPED"
)

var schemaChangeTypes = map[SchemaChangeType]bool{
	Created: true,
	Updated: true,
	Dropped: true,
}

type SchemaChangeTarget string

const (
	Keyspace  SchemaChangeTarget = "KEYSPACE"
	Table     SchemaChangeTarget = "TABLE"
	UserType  SchemaChangeTarget = "TYPE"
	Function  SchemaChangeTarget = "FUNCTION"
	Aggregate SchemaChangeTarget = "AGGREGATE"
)

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

var errorCodes = map[response.ErrorCode]bool{
	response.ErrCodeServer:          true,
	response.ErrCodeProtocol:        true,
	response.ErrCodeCredentials:     true,
	response.ErrCodeUnavailable:     true,
	response.ErrCodeOverloaded:      true,
	response.ErrCodeBootstrapping:   true,
	response.ErrCodeTruncate:        true,
	response.ErrCodeWriteTimeout:    true,
	response.ErrCodeReadTimeout:     true,
	response.ErrCodeReadFailure:     true,
	response.ErrCodeFunctionFailure: true,
	response.ErrCodeWriteFailure:    true,
	response.ErrCodeCDCWriteFailure: true,
	response.ErrCodeCASWriteUnknown: true,
	response.ErrCodeSyntax:          true,
	response.ErrCodeUnauthorized:    true,
	response.ErrCodeInvalid:         true,
	response.ErrCodeConfig:          true,
	response.ErrCodeAlreadyExists:   true,
	response.ErrCodeUnprepared:      true,
}
