package request

import (
	"bytes"
	"scylla-go-driver/frame"
)

const (
	WithNamesForValues = 0x40
)

type Batch struct {
	Type frame.Byte
	Flags frame.Byte
	Queries []BatchQuery
	Consistency frame.Short
	SerialConsistency frame.Short
	Timestamp frame.Long
}

func (q Batch) WriteTo(b *bytes.Buffer) {
	frame.WriteByte(q.Type, b)

	// Write number of queries.
	frame.WriteShort(frame.Short(len(q.Queries)), b)
	for _, k := range q.Queries {
		k.WriteTo(b, q.Flags&WithNamesForValues != 0)
	}
	frame.WriteShort(q.Consistency, b)
	frame.WriteByte(q.Flags, b)
	if q.Flags&serialConsistency != 0 {
		frame.WriteShort(q.SerialConsistency, b)
	}
	if q.Flags&timestamp != 0 {
		frame.WriteLong(q.Timestamp, b)
	}
}


type BatchQuery struct {
	Kind frame.Byte
	Query string
	Prepared frame.Bytes
	Names frame.StringList
	Values []frame.Value
}

func (q BatchQuery) WriteTo(b *bytes.Buffer, name bool) {
	frame.WriteByte(q.Kind, b)
	if q.Kind == 0 {
		frame.WriteLongString(q.Query, b)
	} else {
		frame.WriteShortBytes(q.Prepared, b)
	}

	// Write number of values.
	frame.WriteShort(frame.Short(len(q.Values)), b)
	for i, v := range q.Values {
		if name {
			frame.WriteString(q.Names[i], b)
		}
		frame.WriteValue(v, b)
	}
}
