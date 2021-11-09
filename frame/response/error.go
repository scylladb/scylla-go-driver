// Package response implements functions and types used for handling
// all types of CQL binary protocol responses.
// Reading from buffer is done in Big Endian order.

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

type Unavailable struct {
	Error
	consistency frame.Short
	required    frame.Int
	alive       frame.Int
}

func ReadUnavailable(b *bytes.Buffer) Unavailable {
	return Unavailable{
		Error{
			code:    frame.ReadInt(b),
			message: frame.ReadString(b),
		},
		frame.ReadConsistency(b),
		frame.ReadInt(b),
		frame.ReadInt(b),
	}
}

type WriteTimeout struct {
	Error
	consistency frame.Short
	received    frame.Int
	blockFor    frame.Int
	writeType   string
}

func ReadWriteTimeout(b *bytes.Buffer) WriteTimeout {
	return WriteTimeout{
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

type ReadTimeout struct {
	Error
	consistency frame.Short
	received    frame.Int
	blockFor    frame.Int
	dataPresent frame.Byte
}

func ReadRTimeout(b *bytes.Buffer) ReadTimeout {
	return ReadTimeout{
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

type ReadFailure struct {
	Error
	consistency frame.Short
	received    frame.Int
	blockFor    frame.Int
	numFailures frame.Int
	dataPresent frame.Byte
}

func ReadRFailure(b *bytes.Buffer) ReadFailure {
	return ReadFailure{
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

type FuncFailure struct {
	Error
	keyspace string
	function string
	argTypes frame.StringList
}

func ReadFuncFailure(b *bytes.Buffer) FuncFailure {
	return FuncFailure{
		Error{
			code:    frame.ReadInt(b),
			message: frame.ReadString(b),
		},
		frame.ReadString(b),
		frame.ReadString(b),
		frame.ReadStringList(b),
	}
}

type WriteFailure struct {
	Error
	consistency frame.Short
	received    frame.Int
	blockFor    frame.Int
	numFailures frame.Int
	writeType   string
}

func ReadWriteFailure(b *bytes.Buffer) WriteFailure {
	return WriteFailure{
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

type AlreadyExists struct {
	Error
	keyspace string
	table    string
}

func ReadAlreadyExists(b *bytes.Buffer) AlreadyExists {
	return AlreadyExists{
		Error{
			code:    frame.ReadInt(b),
			message: frame.ReadString(b),
		},
		frame.ReadString(b),
		frame.ReadString(b),
	}
}

type Unprepared struct {
	Error
	unknownID frame.Bytes
}

func ReadUnprepared(b *bytes.Buffer) Unprepared {
	return Unprepared{
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
