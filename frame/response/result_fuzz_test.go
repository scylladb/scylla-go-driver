package response

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

var (
	dummyRR *RowsResult
	dummySK *SetKeyspaceResult
	dummyP  *PreparedResult
	dummySC *SchemaChangeResult
)

// We assign the result to a global variable to avoid compiler optimization.
func FuzzRowsResult(f *testing.F) {
	for _, v := range rowsResultTests {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseRowsResult(&buf)
		dummyRR = out
	})
}

func FuzzSetKeyspaceResult(f *testing.F) {
	for _, v := range setKeyspaceResultTests {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseSetKeyspaceResult(&buf)
		dummySK = out
	})
}

func FuzzPreparedResult(f *testing.F) {
	for _, v := range preparedResultTests {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParsePreparedResult(&buf)
		dummyP = out
	})
}

func FuzzSchemaChangeResultResult(f *testing.F) {
	for _, v := range schemaChangeResultTests {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseSchemaChangeResult(&buf)
		dummySC = out
	})
}
