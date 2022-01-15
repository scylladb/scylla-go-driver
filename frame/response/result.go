package response

import (
	"scylla-go-driver/frame"
)

// Result spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L546
// Below are types of Result with different bodies.

// VoidResult spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L562
type VoidResult struct{}

func ParseVoidResult(_ *frame.Buffer) {}

// RowsResult spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L568
type RowsResult struct {
	Metadata    frame.ResultMetadata
	RowsCnt     frame.Int
	RowsContent []frame.Row
}

func ParseRowsResult(b *frame.Buffer) *RowsResult {
	r := RowsResult{
		Metadata: b.ReadResultMetadata(),
		RowsCnt:  b.ReadInt(),
	}

	r.RowsContent = make([]frame.Row, r.RowsCnt)
	for i := range r.RowsContent {
		r.RowsContent[i] = b.ReadRow(r.Metadata.ColumnsCnt)
	}

	return &r
}

// SetKeyspaceResult spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L669
type SetKeyspaceResult struct {
	Name string
}

func ParseSetKeyspaceResult(b *frame.Buffer) *SetKeyspaceResult {
	return &SetKeyspaceResult{
		Name: b.ReadString(),
	}
}

// PreparedResult spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L675
type PreparedResult struct {
	ID             frame.ShortBytes
	Metadata       frame.PreparedMetadata
	ResultMetadata frame.ResultMetadata
}

func ParsePreparedResult(b *frame.Buffer) *PreparedResult {
	return &PreparedResult{
		ID:             b.ReadShortBytes(),
		Metadata:       b.ReadPreparedMetadata(),
		ResultMetadata: b.ReadResultMetadata(),
	}
}

// SchemaChangeResult spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L742
type SchemaChangeResult struct {
	SchemaChange SchemaChange
}

func ParseSchemaChangeResult(b *frame.Buffer) *SchemaChangeResult {
	return &SchemaChangeResult{
		SchemaChange: *ParseSchemaChange(b),
	}
}
