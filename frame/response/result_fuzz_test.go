package response

import (
	"github.com/mmatczuk/scylla-go-driver/frame"
	"testing"
)

var (
	dummyRR *RowsResult
	dummySK *SetKeyspaceResult
	dummyP  *PreparedResult
	dummySC *SchemaChangeResult
)

// We want to make sure that parsing does not crush driver even for random data.
// We assign result to global variable to avoid compiler optimization.
func FuzzRowsResult(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseRowsResult(&buf)
		dummyRR = out
	})
}

func FuzzSetKeyspaceResult(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseSetKeyspaceResult(&buf)
		dummySK = out
	})
}

func FuzzPreparedResult(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParsePreparedResult(&buf)
		dummyP = out
	})
}

func FuzzSchemaChangeResultResult(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseSchemaChangeResult(&buf)
		dummySC = out
	})
}
