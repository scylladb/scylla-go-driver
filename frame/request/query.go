package request

import (
	"scylla-go-driver/frame"
)

// Query spec https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L337.
type Query struct {
	Query       string
	Consistency frame.Consistency
	Options     QueryOptions
}

// QueryOptions represents optional Values defined by flags.
// Consists of Values required for all flags.
// Values for unset flags are uninitialized.
type QueryOptions struct {
	Flags             frame.Flags
	Values            []frame.Value
	Names             frame.StringList
	PageSize          frame.Int
	PagingState       frame.Bytes
	SerialConsistency frame.Consistency
	Timestamp         frame.Long
}

func (q Query) WriteTo(b *frame.Buffer) {
	b.WriteLongString(q.Query)
	b.WriteConsistency(q.Consistency)
	q.Options.WriteTo(b)
}

func (q QueryOptions) WriteTo(b *frame.Buffer) {
	b.WriteFlags(q.Flags)
	// Checks the flags and writes Values correspondent to the ones that are set.
	if frame.Values&q.Flags != 0 {
		// Writes amount of Values.
		b.WriteShort(frame.Short(len(q.Values)))
		if frame.WithNamesForValues&q.Flags != 0 {
			for i := range q.Names {
				b.WriteString(q.Names[i])
				b.WriteValue(q.Values[i])
			}
		} else {
			for _, v := range q.Values {
				b.WriteValue(v)
			}
		}
	}
	if frame.PageSize&q.Flags != 0 {
		b.WriteInt(q.PageSize)
	}
	if frame.WithPagingState&q.Flags != 0 {
		b.WriteBytes(q.PagingState)
	}
	if frame.WithSerialConsistency&q.Flags != 0 {
		b.WriteConsistency(q.SerialConsistency)
	}
	if frame.WithDefaultTimestamp&q.Flags != 0 {
		b.WriteLong(q.Timestamp)
	}
}
