package response

import (
	"bytes"
	"scylla-go-driver/frame"
)

// Error response message type.
// Used in non specified bellow errors, those
// which don't have a body.
type Error struct {
	Code    frame.Int
	Message string
}

// ReadError reads Error struct from buffer and constructs is.
func ReadError(b *bytes.Buffer) Error {
	return Error{
		Code:    frame.ReadInt(b),
		Message: frame.ReadString(b),
	}
}

type UnavailableErr struct {
	Error
	consistency frame.Short
	required    frame.Int
	alive       frame.Int
}

// ReadUnavailable reads UnavailableErr struct from buffer and constructs is.
func ReadUnavailable(b *bytes.Buffer) UnavailableErr {
	return UnavailableErr{
		Error{
			Code:    frame.ReadInt(b),
			Message: frame.ReadString(b),
		},
		frame.ReadConsistency(b),
		frame.ReadInt(b),
		frame.ReadInt(b),
	}
}

type WriteTimeoutErr struct {
	Error
	consistency frame.Short
	received    frame.Int
	blockFor    frame.Int
	writeType   string
}

// ReadWriteTimeout reads WriteTimeoutErr struct from buffer and constructs is.
func ReadWriteTimeout(b *bytes.Buffer) WriteTimeoutErr {
	return WriteTimeoutErr{
		Error{
			Code:    frame.ReadInt(b),
			Message: frame.ReadString(b),
		},
		frame.ReadConsistency(b),
		frame.ReadInt(b),
		frame.ReadInt(b),
		frame.ReadWriteType(b),
	}
}

type ReadTimeoutErr struct {
	Error
	consistency frame.Short
	received    frame.Int
	blockFor    frame.Int
	dataPresent frame.Byte
}

// ReadRTimeout reads ReadTimeoutErr struct from buffer and constructs is.
func ReadRTimeout(b *bytes.Buffer) ReadTimeoutErr {
	return ReadTimeoutErr{
		Error{
			Code:    frame.ReadInt(b),
			Message: frame.ReadString(b),
		},
		frame.ReadConsistency(b),
		frame.ReadInt(b),
		frame.ReadInt(b),
		frame.ReadByte(b),
	}
}

type ReadFailureErr struct {
	Error
	consistency frame.Short
	received    frame.Int
	blockFor    frame.Int
	numFailures frame.Int
	dataPresent frame.Byte
}

// ReadRFailure reads ReadFailureErr struct from buffer and constructs is.
func ReadRFailure(b *bytes.Buffer) ReadFailureErr {
	return ReadFailureErr{
		Error{
			Code:    frame.ReadInt(b),
			Message: frame.ReadString(b),
		},
		frame.ReadConsistency(b),
		frame.ReadInt(b),
		frame.ReadInt(b),
		frame.ReadInt(b),
		frame.ReadByte(b),
	}
}

type FuncFailureErr struct {
	Error
	keyspace string
	function string
	argTypes frame.StringList
}

// ReadFuncFailure reads FuncFailureErr struct from buffer and constructs is.
func ReadFuncFailure(b *bytes.Buffer) FuncFailureErr {
	return FuncFailureErr{
		Error{
			Code:    frame.ReadInt(b),
			Message: frame.ReadString(b),
		},
		frame.ReadString(b),
		frame.ReadString(b),
		frame.ReadStringList(b),
	}
}

type WriteFailureErr struct {
	Error
	consistency frame.Short
	received    frame.Int
	blockFor    frame.Int
	numFailures frame.Int
	writeType   string
}

// ReadWriteFailure reads WriteFailureErr struct from buffer and constructs is.
func ReadWriteFailure(b *bytes.Buffer) WriteFailureErr {
	return WriteFailureErr{
		Error{
			Code:    frame.ReadInt(b),
			Message: frame.ReadString(b),
		},
		frame.ReadConsistency(b),
		frame.ReadInt(b),
		frame.ReadInt(b),
		frame.ReadInt(b),
		frame.ReadWriteType(b),
	}
}

type AlreadyExistsErr struct {
	Error
	keyspace string
	table    string
}

// ReadAlreadyExists reads AlreadyExistsErr struct from buffer and constructs is.
func ReadAlreadyExists(b *bytes.Buffer) AlreadyExistsErr {
	return AlreadyExistsErr{
		Error{
			Code:    frame.ReadInt(b),
			Message: frame.ReadString(b),
		},
		frame.ReadString(b),
		frame.ReadString(b),
	}
}

type UnpreparedErr struct {
	Error
	unknownID frame.Bytes
}

// ReadUnprepared reads UnpreparedErr struct from buffer and constructs is.
func ReadUnprepared(b *bytes.Buffer) UnpreparedErr {
	return UnpreparedErr{
		Error{
			Code:    frame.ReadInt(b),
			Message: frame.ReadString(b),
		},
		frame.ReadBytes(b),
	}
}

// Types of errors.
const (
	server   frame.Int = 0x0000
	protocol frame.Int = 0x000a
	auth     frame.Int = 0x0100
	// Comment unused to silence linter.
	// unavailable  frame.Int = 0x1000
	overload  frame.Int = 0x1001
	bootstrap frame.Int = 0x1002
	truncate  frame.Int = 0x1003
	// writeTimeout frame.Int = 0x1100
	// readTimeout  frame.Int = 0x1200
	// readFailure  frame.Int = 0x1300
	// funcFailure  frame.Int = 0x1400
	// writeFailure frame.Int = 0x1500
	syntax       frame.Int = 0x2000
	unauthorized frame.Int = 0x2100
	invalid      frame.Int = 0x2200
	config       frame.Int = 0x2300
	// alreadyExits frame.Int = 0x2400
	// unprepared   frame.Int = 0x2500
)
