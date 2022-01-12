package response

import (
	scylla_go_driver "scylla-go-driver/errors"
	"scylla-go-driver/frame"
)

// Error response message type used in non specified errors which don't have a body.
// Error spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1046
type Error struct {
	Code    scylla_go_driver.ErrorCode
	Message string
}

func ParseError(b *frame.Buffer) Error {
	return Error{
		Code:    b.ReadErrorCode(),
		Message: b.ReadString(),
	}
}

// UnavailableError spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1060
type UnavailableError struct {
	Error
	Consistency frame.Consistency
	Required    frame.Int
	Alive       frame.Int
}

func ParseUnavailableError(b *frame.Buffer) UnavailableError {
	return UnavailableError{
		Error: Error{
			Code:    b.ReadErrorCode(),
			Message: b.ReadString(),
		},
		Consistency: b.ReadConsistency(),
		Required:    b.ReadInt(),
		Alive:       b.ReadInt(),
	}
}

// WriteTimeoutError spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1076
type WriteTimeoutError struct {
	Error
	Consistency frame.Consistency
	Received    frame.Int
	BlockFor    frame.Int
	WriteType   frame.WriteType
}

func ParseWriteTimeoutError(b *frame.Buffer) WriteTimeoutError {
	return WriteTimeoutError{
		Error: Error{
			Code:    b.ReadErrorCode(),
			Message: b.ReadString(),
		},
		Consistency: b.ReadConsistency(),
		Received:    b.ReadInt(),
		BlockFor:    b.ReadInt(),
		WriteType:   b.ReadWriteType(),
	}
}

// ReadTimeoutError spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1108
type ReadTimeoutError struct {
	Error
	Consistency frame.Consistency
	Received    frame.Int
	BlockFor    frame.Int
	DataPresent frame.Byte
}

func ParseReadTimeoutError(b *frame.Buffer) ReadTimeoutError {
	return ReadTimeoutError{
		Error: Error{
			Code:    b.ReadErrorCode(),
			Message: b.ReadString(),
		},
		Consistency: b.ReadConsistency(),
		Received:    b.ReadInt(),
		BlockFor:    b.ReadInt(),
		DataPresent: b.ReadByte(),
	}
}

// ReadFailureError spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1125
type ReadFailureError struct {
	Error
	Consistency frame.Consistency
	Received    frame.Int
	BlockFor    frame.Int
	NumFailures frame.Int
	DataPresent frame.Byte
}

func ParseReadFailureError(b *frame.Buffer) ReadFailureError {
	return ReadFailureError{
		Error: Error{
			Code:    b.ReadErrorCode(),
			Message: b.ReadString(),
		},
		Consistency: b.ReadConsistency(),
		Received:    b.ReadInt(),
		BlockFor:    b.ReadInt(),
		NumFailures: b.ReadInt(),
		DataPresent: b.ReadByte(),
	}
}

// FuncFailureError spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1140
type FuncFailureError struct {
	Error
	Keyspace string
	Function string
	ArgTypes frame.StringList
}

func ParseFuncFailureError(b *frame.Buffer) FuncFailureError {
	return FuncFailureError{
		Error: Error{
			Code:    b.ReadErrorCode(),
			Message: b.ReadString(),
		},
		Keyspace: b.ReadString(),
		Function: b.ReadString(),
		ArgTypes: b.ReadStringList(),
	}
}

// WriteFailureError spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1147
type WriteFailureError struct {
	Error
	Consistency frame.Consistency
	Received    frame.Int
	BlockFor    frame.Int
	NumFailures frame.Int
	WriteType   frame.WriteType
}

func ParseWriteFailureError(b *frame.Buffer) WriteFailureError {
	return WriteFailureError{
		Error: Error{
			Code:    b.ReadErrorCode(),
			Message: b.ReadString(),
		},
		Consistency: b.ReadConsistency(),
		Received:    b.ReadInt(),
		BlockFor:    b.ReadInt(),
		NumFailures: b.ReadInt(),
		WriteType:   b.ReadWriteType(),
	}
}

// AlreadyExistsError spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1187
type AlreadyExistsError struct {
	Error
	Keyspace string
	Table    string
}

func ParseAlreadyExistsError(b *frame.Buffer) AlreadyExistsError {
	return AlreadyExistsError{
		Error: Error{
			Code:    b.ReadErrorCode(),
			Message: b.ReadString(),
		},
		Keyspace: b.ReadString(),
		Table:    b.ReadString(),
	}
}

// UnpreparedError spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1197
type UnpreparedError struct {
	Error
	UnknownID frame.ShortBytes
}

func ParseUnpreparedError(b *frame.Buffer) UnpreparedError {
	return UnpreparedError{
		Error: Error{
			Code:    b.ReadErrorCode(),
			Message: b.ReadString(),
		},
		UnknownID: b.ReadShortBytes(),
	}
}
