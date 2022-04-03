package response

import (
	"github.com/google/go-cmp/cmp"

	"github.com/mmatczuk/scylla-go-driver/frame"

	"testing"
)

var (
	columns = []frame.ColumnSpec{
		{
			Keyspace: "ks1",
			Table:    "table1",
			Name:     "column1",
			Type:     frame.Option{ID: 3},
		},
		{
			Keyspace: "ks1",
			Table:    "table1",
			Name:     "column2",
			Type:     frame.Option{ID: 3},
		},
	}

	columnsBytes = func(b *frame.Buffer) {
		b.WriteString("ks1")
		b.WriteString("table1")
		b.WriteString("column1")
		b.WriteShort(3)
		b.WriteString("ks1")
		b.WriteString("table1")
		b.WriteString("column2")
		b.WriteShort(3)
	}

	globalColumns = []frame.ColumnSpec{
		{
			Name: "column1",
			Type: frame.Option{ID: 3},
		},
		{
			Name: "column2",
			Type: frame.Option{ID: 3},
		},
	}

	globalColumnsBytes = func(b *frame.Buffer) {
		b.WriteString("column1")
		b.WriteShort(3)
		b.WriteString("column2")
		b.WriteShort(3)
	}

	rowsContent = []frame.Row{
		{frame.CqlFromBlob([]byte{0x11}), frame.CqlFromBlob([]byte{0x12})},
		{frame.CqlFromBlob([]byte{0x21}), frame.CqlFromBlob([]byte{0x22})},
		{frame.CqlFromBlob([]byte{0x31}), frame.CqlFromBlob([]byte{0x32})},
	}

	rowsContentBytes = func(b *frame.Buffer) {
		b.WriteInt(1)
		b.WriteByte(0x11)
		b.WriteInt(1)
		b.WriteByte(0x12)
		b.WriteInt(1)
		b.WriteByte(0x21)
		b.WriteInt(1)
		b.WriteByte(0x22)
		b.WriteInt(1)
		b.WriteByte(0x31)
		b.WriteInt(1)
		b.WriteByte(0x32)
	}
)

func TestRowsResult(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected RowsResult
	}{
		{
			name: "2 blobs",
			content: func() []byte {
				var b frame.Buffer
				b.WriteInt(0) // flag
				b.WriteInt(2) // columns count
				columnsBytes(&b)
				b.WriteInt(3) // rows count
				rowsContentBytes(&b)
				return b.Bytes()
			}(),
			expected: RowsResult{
				Metadata: frame.ResultMetadata{
					Flags:      0,
					ColumnsCnt: 2,
					Columns:    columns,
				},
				RowsCnt:     3,
				RowsContent: rowsContent,
			}},
		// {
		// 	name: "no metadata",
		// 	content: func() []byte {
		// 		var b frame.Buffer
		// 		b.WriteInt(4) // flag
		// 		b.WriteInt(2) // columns count
		// 		b.WriteInt(3) // rows count
		// 		rowsContentBytes(&b)
		// 		return b.Bytes()
		// 	}(),
		// 	expected: RowsResult{
		// 		Metadata: frame.ResultMetadata{
		// 			Flags:      4,
		// 			ColumnsCnt: 2,
		// 		},
		// 		RowsCnt:     3,
		// 		RowsContent: rowsContent,
		// 	}},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			a := ParseRowsResult(&buf)
			if diff := cmp.Diff(*a, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestSetKeyspaceResult(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected SetKeyspaceResult
	}{
		{
			name: "simple keyspace",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString("ks")
				return b.Bytes()
			}(),
			expected: SetKeyspaceResult{Name: "ks"},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			a := ParseSetKeyspaceResult(&buf)
			if diff := cmp.Diff(*a, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestPreparedResult(t *testing.T) {
	t.Parallel()

	pqID := frame.Bytes{0xca, 0xfe, 0xba, 0xbe}
	pqFun := func(b *frame.Buffer) {
		b.WriteByte(0xca)
		b.WriteByte(0xfe)
		b.WriteByte(0xba)
		b.WriteByte(0xbe)
	}

	testCases := []struct {
		name     string
		content  []byte
		expected PreparedResult
	}{
		{
			name: "metadata with 2 variables",
			content: func() []byte {
				var b frame.Buffer
				b.WriteShort(4)
				pqFun(&b)
				b.WriteInt(0) // metadata flag
				b.WriteInt(2) // metadata columns count
				b.WriteInt(0) // metadata pk count
				columnsBytes(&b)
				b.WriteInt(0) // result metadata flag
				b.WriteInt(0) // result metadata columns count
				return b.Bytes()
			}(),
			expected: PreparedResult{
				ID: pqID,
				Metadata: frame.PreparedMetadata{
					Flags:      0,
					ColumnsCnt: 2,
					PkCnt:      0,
					PkIndexes:  []frame.Short{},
					Columns:    columns,
				},
				ResultMetadata: frame.ResultMetadata{
					Flags:      0,
					ColumnsCnt: 0,
					Columns:    []frame.ColumnSpec{},
				},
			},
		},
		{
			name: "variables metadata with 2 columns",
			content: func() []byte {
				var b frame.Buffer
				b.WriteShort(4)
				pqFun(&b)
				b.WriteInt(0) // metadata flag
				b.WriteInt(0) // metadata columns count
				b.WriteInt(0) // metadata pk count
				b.WriteInt(0) // result metadata flag
				b.WriteInt(2) // result metadata columns count
				columnsBytes(&b)
				return b.Bytes()
			}(),
			expected: PreparedResult{
				ID: pqID,
				Metadata: frame.PreparedMetadata{
					Flags:      0,
					ColumnsCnt: 0,
					PkCnt:      0,
					PkIndexes:  []frame.Short{},
					Columns:    []frame.ColumnSpec{},
				},
				ResultMetadata: frame.ResultMetadata{
					Flags:      0,
					ColumnsCnt: 2,
					Columns:    columns,
				},
			},
		},
		{
			name: "variables metadata with 2 variables and 1 pk index",
			content: func() []byte {
				var b frame.Buffer
				b.WriteShort(4)
				pqFun(&b)
				b.WriteInt(0x0001)      // metadata flag
				b.WriteInt(2)           // metadata columns count
				b.WriteInt(1)           // metadata pk count
				b.WriteShort(1)         // pk index
				b.WriteString("ks1")    // global keyspace
				b.WriteString("table1") // global table
				globalColumnsBytes(&b)
				b.WriteInt(0) // result metadata flag
				b.WriteInt(0) // result metadata columns count
				return b.Bytes()
			}(),
			expected: PreparedResult{
				ID: pqID,
				Metadata: frame.PreparedMetadata{
					Flags:          0x0001,
					ColumnsCnt:     2,
					PkCnt:          1,
					PkIndexes:      []frame.Short{1},
					GlobalKeyspace: "ks1",
					GlobalTable:    "table1",
					Columns:        globalColumns,
				},
				ResultMetadata: frame.ResultMetadata{
					Flags:      0,
					ColumnsCnt: 0,
					Columns:    []frame.ColumnSpec{},
				},
			},
		},
		{
			name: "variables metadata with no variables",
			content: func() []byte {
				var b frame.Buffer
				b.WriteShort(4)
				pqFun(&b)
				b.WriteInt(0)      // metadata flag
				b.WriteInt(0)      // metadata columns count
				b.WriteInt(0)      // metadata pk count
				b.WriteInt(0x0001) // result metadata flag
				b.WriteInt(2)      // result metadata columns count
				b.WriteString("ks1")
				b.WriteString("table1")
				globalColumnsBytes(&b)
				return b.Bytes()
			}(),
			expected: PreparedResult{
				ID: pqID,
				Metadata: frame.PreparedMetadata{
					Flags:      0,
					ColumnsCnt: 0,
					PkCnt:      0,
					PkIndexes:  []frame.Short{},
					Columns:    []frame.ColumnSpec{},
				},
				ResultMetadata: frame.ResultMetadata{
					Flags:          0x0001,
					ColumnsCnt:     2,
					GlobalKeyspace: "ks1",
					GlobalTable:    "table1",
					Columns:        globalColumns,
				},
			},
		},
		{
			name: "variables metadata with 2 variables and 1 pk index",
			content: func() []byte {
				var b frame.Buffer
				b.WriteShort(4)
				pqFun(&b)
				b.WriteInt(0x0001)      // metadata flag
				b.WriteInt(2)           // metadata columns count
				b.WriteInt(1)           // metadata pk count
				b.WriteShort(1)         // pk index
				b.WriteString("ks1")    // global keyspace
				b.WriteString("table1") // global table
				globalColumnsBytes(&b)
				b.WriteInt(0) // result metadata flag
				b.WriteInt(0) // result metadata columns count
				return b.Bytes()
			}(),
			expected: PreparedResult{
				ID: pqID,
				Metadata: frame.PreparedMetadata{
					Flags:          0x0001,
					ColumnsCnt:     2,
					PkCnt:          1,
					PkIndexes:      []frame.Short{1},
					GlobalKeyspace: "ks1",
					GlobalTable:    "table1",
					Columns:        globalColumns,
				},
				ResultMetadata: frame.ResultMetadata{
					Flags:      0,
					ColumnsCnt: 0,
					Columns:    []frame.ColumnSpec{},
				},
			},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			a := ParsePreparedResult(&buf)
			if diff := cmp.Diff(*a, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestSchemaChangeResult(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected SchemaChangeResult
	}{
		{
			name: "create keyspace",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString(string(frame.Created))
				b.WriteString(string(frame.Keyspace))
				b.WriteString("test")
				return b.Bytes()
			}(),
			expected: SchemaChangeResult{SchemaChange: SchemaChange{
				Change:   frame.Created,
				Target:   frame.Keyspace,
				Keyspace: "test",
			}},
		},
		{
			name: "create table",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString(string(frame.Created))
				b.WriteString(string(frame.Table))
				b.WriteString("test")
				b.WriteString("mytable")
				return b.Bytes()
			}(),
			expected: SchemaChangeResult{SchemaChange: SchemaChange{
				Change:   frame.Created,
				Target:   frame.Table,
				Keyspace: "test",
				Object:   "mytable",
			}},
		},
		{
			name: "create type",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString(string(frame.Created))
				b.WriteString(string(frame.UserType))
				b.WriteString("test")
				b.WriteString("mytable")
				b.WriteShort(0)
				return b.Bytes()
			}(),
			expected: SchemaChangeResult{SchemaChange: SchemaChange{
				Change:   frame.Created,
				Target:   frame.UserType,
				Keyspace: "test",
				Object:   "mytable",
			}},
		},
		{
			name: "create function",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString(string(frame.Created))
				b.WriteString(string(frame.Function))
				b.WriteString("test")
				b.WriteString("myfunction")
				b.WriteShort(2)
				b.WriteString("int")
				b.WriteString("int")
				return b.Bytes()
			}(),
			expected: SchemaChangeResult{SchemaChange: SchemaChange{
				Change:    frame.Created,
				Target:    frame.Function,
				Keyspace:  "test",
				Object:    "myfunction",
				Arguments: frame.StringList{"int", "int"},
			}},
		},
		{
			name: "create aggregate",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString(string(frame.Created))
				b.WriteString(string(frame.Aggregate))
				b.WriteString("test")
				b.WriteString("myaggregate")
				b.WriteShort(2)
				b.WriteString("int")
				b.WriteString("int")
				return b.Bytes()
			}(),
			expected: SchemaChangeResult{SchemaChange: SchemaChange{
				Change:    frame.Created,
				Target:    frame.Aggregate,
				Keyspace:  "test",
				Object:    "myaggregate",
				Arguments: frame.StringList{"int", "int"},
			}},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			a := ParseSchemaChangeResult(&buf)
			if diff := cmp.Diff(*a, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
