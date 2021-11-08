package frame

import (
	"errors"
)

type (
	Byte = byte

	Short = int16
	Int   = int32
	Long  = int64

	UUID           = [16]byte
	StringMultiMap = map[string][]string
	StringList     = []string

	OpCode = byte

	Header struct {
		Version  Byte
		Flags    Byte
		StreamID Short
		Opcode   OpCode
		Length   Int
	}
)

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

	CQLv4 Byte = 0x84
)

var (
	protocolVersionErr = errors.New("frame protocol version is not supported")
)
