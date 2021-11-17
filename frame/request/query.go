package request

import (
	"bytes"
	"scylla-go-driver/frame"
)

// Flags used inside Query.
const (
	values            = 0x01
	skipMetadata      = 0x02
	pageSize          = 0x04
	pagingState       = 0x08
	serialConsistency = 0x10
	timestamp         = 0x20
	namedValues       = 0x40
)

// Query request type message.
type Query struct {
	Query       string
	Consistency frame.Short
	Options     QueryOptions
}

// QueryOptions represents optional values defined by flags.
// Consists of values required for all flags.
// Values for unset flags are uninitialized.
type QueryOptions struct {
	Flags             frame.Byte
	Values            []frame.Value
	Names             []string
	PageSize          frame.Int
	PagingState       frame.Bytes
	SerialConsistency frame.Short
	Timestamp         frame.Long
}

// Write writes Query to the buffer.
func (q Query) Write(b *bytes.Buffer) {
	frame.WriteLongString(q.Query, b)
	frame.WriteShort(q.Consistency, b)
	q.Options.Write(b)
}

// Write writes QueryOptions to the buffer.
func (q QueryOptions) Write(b *bytes.Buffer) {
	frame.WriteByte(q.Flags, b)
	// Checks the flags and writes values correspondent to the ones that are set.
	if values&q.Flags != 0 {
		// Writes amount of values.
		frame.WriteShort(frame.Short(len(q.Values)), b)
		if namedValues&q.Flags != 0 {
			for i := range q.Names {
				frame.WriteString(q.Names[i], b)
				frame.WriteValue(q.Values[i], b)
			}
		} else {
			for _, v := range q.Values {
				frame.WriteValue(v, b)
			}
		}
	}
	if pageSize&q.Flags != 0 {
		frame.WriteInt(q.PageSize, b)
	}
	if pagingState&q.Flags != 0 {
		frame.WriteBytes(q.PagingState, b)
	}
	if serialConsistency&q.Flags != 0 {
		frame.WriteShort(q.SerialConsistency, b)
	}
	if timestamp&q.Flags != 0 {
		frame.WriteLong(q.Timestamp, b)
	}
}
