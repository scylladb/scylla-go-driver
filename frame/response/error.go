package response

import (
	"scylla-go-driver/frame"
)

// Error response message type.
// Used in non specified bellow errors, those
// which don't have a body.
type Error struct {
	Code    frame.Int
	Message string
}

// ParseError reads Error struct from buffer and constructs is.
func ParseError(b *frame.Buffer) (Error, error) {
	return Error{
		Code:    b.ParseErrorCode(),
		Message: b.ReadString(),
	}, b.Error()
}

type UnavailableErr struct {
	Error
	Consistency frame.Consistency
	Required    frame.Int
	Alive       frame.Int
}

// ParseUnavailable reads UnavailableErr struct from buffer and constructs is.
func ParseUnavailable(b *frame.Buffer) (UnavailableErr, error) {
	return UnavailableErr{
		Error: Error{
			Code:    b.ParseErrorCode(),
			Message: b.ReadString(),
		},
		Consistency: b.ParseConsistency(),
		Required: b.ReadInt(),
		Alive: b.ReadInt(),
	}, b.Error()
}

type WriteTimeoutErr struct {
	Error
	Consistency frame.Consistency
	Received    frame.Int
	BlockFor    frame.Int
	WriteType   frame.WriteType
}

// ParseWriteTimeout reads WriteTimeoutErr struct from buffer and constructs is.
func ParseWriteTimeout(b *frame.Buffer) (WriteTimeoutErr, error) {
	return WriteTimeoutErr{
		Error: Error{
			Code:    b.ParseErrorCode(),
			Message: b.ReadString(),
		},
		Consistency: b.ParseConsistency(),
		Received: b.ReadInt(),
		BlockFor: b.ReadInt(),
		WriteType: b.ParseWriteType(),
	}, b.Error()
}

type ReadTimeoutErr struct {
	Error
	Consistency frame.Consistency
	Received    frame.Int
	BlockFor    frame.Int
	DataPresent frame.Byte
}

// ParseReadTimeout reads ReadTimeoutErr struct from buffer and constructs is.
func ParseReadTimeout(b *frame.Buffer) (ReadTimeoutErr, error) {
	return ReadTimeoutErr{
		Error: Error{
			Code:    b.ParseErrorCode(),
			Message: b.ReadString(),
		},
		Consistency: b.ParseConsistency(),
		Received: b.ReadInt(),
		BlockFor: b.ReadInt(),
		DataPresent: b.ReadByte(),
	}, b.Error()
}

type ReadFailureErr struct {
	Error
	Consistency frame.Consistency
	Received    frame.Int
	BlockFor    frame.Int
	NumFailures frame.Int
	DataPresent frame.Byte
}

// ParseReadFailure reads ReadFailureErr struct from buffer and constructs is.
func ParseReadFailure(b *frame.Buffer) (ReadFailureErr, error) {
	return ReadFailureErr{
		Error: Error{
			Code:    b.ParseErrorCode(),
			Message: b.ReadString(),
		},
		Consistency: b.ParseConsistency(),
		Received: b.ReadInt(),
		BlockFor: b.ReadInt(),
		NumFailures: b.ReadInt(),
		DataPresent: b.ReadByte(),
	}, b.Error()
}

type FuncFailureErr struct {
	Error
	Keyspace string
	Function string
	ArgTypes frame.StringList
}

// ParseFuncFailure reads FuncFailureErr struct from buffer and constructs is.
func ParseFuncFailure(b *frame.Buffer) FuncFailureErr {
	return FuncFailureErr{
		Error: Error{
			Code:    b.ParseErrorCode(),
			Message: b.ReadString(),
		},
		Keyspace: b.ReadString(),
		Function: b.ReadString(),
		ArgTypes: b.ReadStringList(),
	}
}

type WriteFailureErr struct {
	Error
	Consistency frame.Consistency
	Received    frame.Int
	BlockFor    frame.Int
	NumFailures frame.Int
	WriteType   frame.WriteType
}

// ParseWriteFailure reads WriteFailureErr struct from buffer and constructs is.
func ParseWriteFailure(b *frame.Buffer) (WriteFailureErr, error) {
	return WriteFailureErr{
		Error: Error{
			Code:    b.ParseErrorCode(),
			Message: b.ReadString(),
		},
		Consistency: b.ParseConsistency(),
		Received: b.ReadInt(),
		BlockFor: b.ReadInt(),
		NumFailures: b.ReadInt(),
		WriteType: b.ParseWriteType(),
	}, b.Error()
}

type AlreadyExistsErr struct {
	Error
	Keyspace string
	Table    string
}

// ParseAlreadyExists reads AlreadyExistsErr struct from buffer and constructs is.
func ParseAlreadyExists(b *frame.Buffer) (AlreadyExistsErr, error) {
	return AlreadyExistsErr{
		Error: Error{
			Code:    b.ParseErrorCode(),
			Message: b.ReadString(),
		},
		Keyspace: b.ReadString(),
		Table: b.ReadString(),
	}, b.Error()
}

type UnpreparedErr struct {
	Error
	UnknownID frame.Bytes
}

// ParseUnprepared reads UnpreparedErr struct from buffer and constructs is.
func ParseUnprepared(b *frame.Buffer) (UnpreparedErr, error) {
	return UnpreparedErr{
		Error: Error{
			Code:    b.ParseErrorCode(),
			Message: b.ReadString(),
		},
		UnknownID: b.ReadShortBytes(),
	}, b.Error()
}
