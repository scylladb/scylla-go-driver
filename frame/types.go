package frame

import (
	"errors"
)

// Generic types from CQL binary protocol.
type (
	Byte  = byte
	Short = uint16
	Int   = int32
	Long  = int64

	UUID           = [16]byte
	StringMultiMap = map[string][]string
	StringMap      = map[string]string
	StringList     = []string

	Value struct {
		N     Int
		Bytes Bytes
	}

	Inet struct {
		IP   Bytes
		Port Int
	}

	Bytes = []byte

	OpCode = byte
)

// Types of messages.
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

// Types of consistencies.
const (
	ANY          Short = 0x0000
	ONE          Short = 0x0001
	TWO          Short = 0x0002
	THREE        Short = 0x0003
	QUORUM       Short = 0x0004
	ALL          Short = 0x0005
	LOCAL_QUORUM Short = 0x0006
	EACH_QUORUM  Short = 0x0007
	SERIAL       Short = 0x0008
	LOCAL_SERIAL Short = 0x0009
	LOCAL_ONE    Short = 0x000A
)

// CQLv4 is the only protocol version currently supported.
const CQLv4 Byte = 0x84

// Errors that might occur during parsing.
var (
	protocolVersionErr    = errors.New("frame protocol version is not supported")
	unknownConsistencyErr = errors.New("unknown consistency")
	unknownWriteTypeErr   = errors.New("unknown write type")
	invalidValueLength    = errors.New("valid Value length is greater from or equal to -2")
	invalidIPLength       = errors.New("the only valid IP lengths are either 4 (IP4) or 16 (IP6)")
)
