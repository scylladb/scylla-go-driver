package request

import (
	"scylla-go-driver/frame"
)

const (
	// Flag for BatchQuery. Values will have its names.
	WithNamesForValues = 0x40
)

// Batch spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L414
type Batch struct {
	Type              frame.Byte
	Flags             frame.Byte
	Queries           []BatchQuery
	Consistency       frame.Short
	SerialConsistency frame.Short
	Timestamp         frame.Long
}

// WriteTo writes Batch body into bytes.Buffer.
//TODO: probably move part of this function into frame.
func (q Batch) WriteTo(b *frame.Buffer) {
	b.WriteByte(q.Type)

	// WriteTo number of queries.
	b.WriteShort(frame.Short(len(q.Queries)))
	for _, k := range q.Queries {
		k.WriteTo(b, q.Flags&WithNamesForValues != 0)
	}
	b.WriteShort(q.Consistency)
	b.WriteByte(q.Flags)
	if q.Flags&frame.WithSerialConsistency != 0 {
		b.WriteShort(q.SerialConsistency)
	}
	if q.Flags&frame.WithDefaultTimestamp != 0 {
		b.WriteLong(q.Timestamp)
	}
}

// BatchQuery spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L452
type BatchQuery struct {
	Kind     frame.Byte
	Query    string
	Prepared frame.Bytes
	Names    frame.StringList
	Values   []frame.Value
}

// TODO: as above?
func (q BatchQuery) WriteTo(b *frame.Buffer, name bool) {
	b.WriteByte(q.Kind)
	if q.Kind == 0 {
		b.WriteLongString(q.Query)
	} else {
		b.WriteShortBytes(q.Prepared)
	}

	// WriteTo number of Values.
	b.WriteShort(frame.Short(len(q.Values)))
	for i, v := range q.Values {
		if name {
			b.WriteString(q.Names[i])
		}
		b.WriteValue(v)
	}
}
