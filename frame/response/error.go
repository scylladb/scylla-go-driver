package response

import (
	"bytes"
	"scylla-go-driver/frame"
)

// Error response message type.
type Error struct {
	code    frame.Int
	message string
}

func ReadError(b *bytes.Buffer) Error {
	return Error{
		code:    frame.ReadInt(b),
		message: frame.ReadString(b),
	}
}

type UnavailableErr struct {
	Error
	consistency frame.Short
	required    frame.Int
	alive       frame.Int
}

func ReadUnavailable(b *bytes.Buffer) UnavailableErr {
	return UnavailableErr{
		Error{
			code:    frame.ReadInt(b),
			message: frame.ReadString(b),
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

func ReadWriteTimeout(b *bytes.Buffer) WriteTimeoutErr {
	return WriteTimeoutErr{
		Error{
			code:    frame.ReadInt(b),
			message: frame.ReadString(b),
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

func ReadRTimeout(b *bytes.Buffer) ReadTimeoutErr {
	return ReadTimeoutErr{
		Error{
			code:    frame.ReadInt(b),
			message: frame.ReadString(b),
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

func ReadRFailure(b *bytes.Buffer) ReadFailureErr {
	return ReadFailureErr{
		Error{
			code:    frame.ReadInt(b),
			message: frame.ReadString(b),
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

func ReadFuncFailure(b *bytes.Buffer) FuncFailureErr {
	return FuncFailureErr{
		Error{
			code:    frame.ReadInt(b),
			message: frame.ReadString(b),
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

func ReadWriteFailure(b *bytes.Buffer) WriteFailureErr {
	return WriteFailureErr{
		Error{
			code:    frame.ReadInt(b),
			message: frame.ReadString(b),
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

func ReadAlreadyExists(b *bytes.Buffer) AlreadyExistsErr {
	return AlreadyExistsErr{
		Error{
			code:    frame.ReadInt(b),
			message: frame.ReadString(b),
		},
		frame.ReadString(b),
		frame.ReadString(b),
	}
}

type UnpreparedErr struct {
	Error
	unknownID frame.Bytes
}

func ReadUnprepared(b *bytes.Buffer) UnpreparedErr {
	return UnpreparedErr{
		Error{
			code:    frame.ReadInt(b),
			message: frame.ReadString(b),
		},
		frame.ReadBytes(b),
	}
}

// Types of errors.
const (
	server       frame.Int = 0x0000
	protocol     frame.Int = 0x000a
	auth         frame.Int = 0x0100
	unavailable  frame.Int = 0x1000
	overload     frame.Int = 0x1001
	bootstrap    frame.Int = 0x1002
	truncate     frame.Int = 0x1003
	writeTimeout frame.Int = 0x1100
	readTimeout  frame.Int = 0x1200
	readFailure  frame.Int = 0x1300
	funcFailure  frame.Int = 0x1400
	writeFailure frame.Int = 0x1500
	syntax       frame.Int = 0x2000
	unauthorized frame.Int = 0x2100
	invalid      frame.Int = 0x2200
	config       frame.Int = 0x2300
	alreadyExits frame.Int = 0x2400
	unprepared   frame.Int = 0x2500
)
