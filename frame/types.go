package frame

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

const ConsistencyRange = 10

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

type ErrorCode = Int

// See CQL Binary Protocol v5, section 8 for more details.
// https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec
const (
	// ErrCodeServer indicates unexpected error on server-side.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1246-L1247
	ErrCodeServer ErrorCode = 0x0000

	// ErrCodeProtocol indicates a protocol violation by some client message.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1248-L1250
	ErrCodeProtocol ErrorCode = 0x000A

	// ErrCodeCredentials indicates missing required authentication.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1251-L1254
	ErrCodeCredentials ErrorCode = 0x0100

	// ErrCodeUnavailable indicates unavailable error.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1255-L1265
	ErrCodeUnavailable ErrorCode = 0x1000

	// ErrCodeOverloaded returned in case of request on overloaded node coordinator.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1266-L1267
	ErrCodeOverloaded ErrorCode = 0x1001

	// ErrCodeBootstrapping returned from the coordinator node in bootstrapping phase.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1268-L1269
	ErrCodeBootstrapping ErrorCode = 0x1002

	// ErrCodeTruncate indicates truncation exception.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1270
	ErrCodeTruncate ErrorCode = 0x1003

	// ErrCodeWriteTimeout returned in case of timeout during the request write.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1271-L1304
	ErrCodeWriteTimeout ErrorCode = 0x1100

	// ErrCodeReadTimeout returned in case of timeout during the request read.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1305-L1321
	ErrCodeReadTimeout ErrorCode = 0x1200

	// ErrCodeReadFailure indicates request read error which is not covered by ErrCodeReadTimeout.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1322-L1340
	ErrCodeReadFailure ErrorCode = 0x1300

	// ErrCodeFunctionFailure indicates an error in user-defined function.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1341-L1347
	ErrCodeFunctionFailure ErrorCode = 0x1400

	// ErrCodeWriteFailure indicates request write error which is not covered by ErrCodeWriteTimeout.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1348-L1385
	ErrCodeWriteFailure ErrorCode = 0x1500

	// ErrCodeCDCWriteFailure is defined, but not yet documented in CQLv5 protocol.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1386
	ErrCodeCDCWriteFailure ErrorCode = 0x160
	// ErrCodeCASWriteUnknown indicates only partially completed CAS operation.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1387-L1397
	ErrCodeCASWriteUnknown ErrorCode = 0x1700

	// ErrCodeSyntax indicates the syntax error in the query.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1399
	ErrCodeSyntax ErrorCode = 0x2000

	// ErrCodeUnauthorized indicates access rights violation by user on performed operation.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1400-L1401
	ErrCodeUnauthorized ErrorCode = 0x2100

	// ErrCodeInvalid indicates invalid query error which is not covered by ErrCodeSyntax.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1402
	ErrCodeInvalid ErrorCode = 0x2200

	// ErrCodeConfig indicates the configuration error.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1403
	ErrCodeConfig ErrorCode = 0x2300

	// ErrCodeAlreadyExists is returned for the requests creating the existing keyspace/table.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1404-L1413
	ErrCodeAlreadyExists ErrorCode = 0x2400

	// ErrCodeUnprepared returned from the host for prepared statement which is unknown.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1414-L1417
	ErrCodeUnprepared ErrorCode = 0x2500
)


var errorCodes = map[ErrorCode]bool{
	ErrCodeServer:          true,
	ErrCodeProtocol:        true,
	ErrCodeCredentials:     true,
	ErrCodeUnavailable:     true,
	ErrCodeOverloaded:      true,
	ErrCodeBootstrapping:   true,
	ErrCodeTruncate:        true,
	ErrCodeWriteTimeout:    true,
	ErrCodeReadTimeout:     true,
	ErrCodeReadFailure:     true,
	ErrCodeFunctionFailure: true,
	ErrCodeWriteFailure:    true,
	ErrCodeCDCWriteFailure: true,
	ErrCodeCASWriteUnknown: true,
	ErrCodeSyntax:          true,
	ErrCodeUnauthorized:    true,
	ErrCodeInvalid:         true,
	ErrCodeConfig:          true,
	ErrCodeAlreadyExists:   true,
	ErrCodeUnprepared:      true,
}
