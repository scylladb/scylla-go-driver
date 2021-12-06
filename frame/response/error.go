package response

import (
	"scylla-go-driver/frame"
)

// Error response message type used in non specified errors which don't have a body.
// Error spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1046
type Error struct {
	Code    frame.ErrorCode
	Message string
}

func ParseError(b *frame.Buffer) Error {
	return Error{
		Code:    b.ReadErrorCode(),
		Message: b.ReadString(),
	}
}

// UnavailableErr spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1060
type UnavailableErr struct {
	Error
	Consistency frame.Consistency
	Required    frame.Int
	Alive       frame.Int
}

func ParseUnavailable(b *frame.Buffer) UnavailableErr {
	return UnavailableErr{
		Error: Error{
			Code:    b.ReadErrorCode(),
			Message: b.ReadString(),
		},
		Consistency: b.ReadConsistency(),
		Required:    b.ReadInt(),
		Alive:       b.ReadInt(),
	}
}

// WriteTimeoutErr spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1076
type WriteTimeoutErr struct {
	Error
	Consistency frame.Consistency
	Received    frame.Int
	BlockFor    frame.Int
	WriteType   frame.WriteType
}

func ParseWriteTimeout(b *frame.Buffer) WriteTimeoutErr {
	return WriteTimeoutErr{
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

// ReadTimeoutErr spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1108
type ReadTimeoutErr struct {
	Error
	Consistency frame.Consistency
	Received    frame.Int
	BlockFor    frame.Int
	DataPresent frame.Byte
}

func ParseReadTimeout(b *frame.Buffer) ReadTimeoutErr {
	return ReadTimeoutErr{
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

// ReadFailureErr spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1125
type ReadFailureErr struct {
	Error
	Consistency frame.Consistency
	Received    frame.Int
	BlockFor    frame.Int
	NumFailures frame.Int
	DataPresent frame.Byte
}

func ParseReadFailure(b *frame.Buffer) ReadFailureErr {
	return ReadFailureErr{
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

// FuncFailureErr spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1140
type FuncFailureErr struct {
	Error
	Keyspace string
	Function string
	ArgTypes frame.StringList
}

func ParseFuncFailure(b *frame.Buffer) FuncFailureErr {
	return FuncFailureErr{
		Error: Error{
			Code:    b.ReadErrorCode(),
			Message: b.ReadString(),
		},
		Keyspace: b.ReadString(),
		Function: b.ReadString(),
		ArgTypes: b.ReadStringList(),
	}
}

// WriteFailureErr spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1147
type WriteFailureErr struct {
	Error
	Consistency frame.Consistency
	Received    frame.Int
	BlockFor    frame.Int
	NumFailures frame.Int
	WriteType   frame.WriteType
}

func ParseWriteFailure(b *frame.Buffer) WriteFailureErr {
	return WriteFailureErr{
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

// AlreadyExistsErr spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1187
type AlreadyExistsErr struct {
	Error
	Keyspace string
	Table    string
}

func ParseAlreadyExists(b *frame.Buffer) AlreadyExistsErr {
	return AlreadyExistsErr{
		Error: Error{
			Code:    b.ReadErrorCode(),
			Message: b.ReadString(),
		},
		Keyspace: b.ReadString(),
		Table:    b.ReadString(),
	}
}

// UnpreparedErr spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1197
type UnpreparedErr struct {
	Error
	UnknownID frame.Bytes
}

func ParseUnprepared(b *frame.Buffer) UnpreparedErr {
	return UnpreparedErr{
		Error: Error{
			Code:    b.ReadErrorCode(),
			Message: b.ReadString(),
		},
		UnknownID: b.ReadShortBytes(),
	}
}
