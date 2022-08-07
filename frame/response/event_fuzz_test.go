package response

import (
	"testing"

	"github.com/scylladb/scylla-go-driver/frame"
)

var (
	dummyStaC *StatusChange
	dummyTopC *TopologyChange
	dummySchC *SchemaChange
)

// We assign the result to a global variable to avoid compiler optimization.
func FuzzStatusChange(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseStatusChange(&buf)
		dummyStaC = out
	})
}

func FuzzTopologyChange(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseTopologyChange(&buf)
		dummyTopC = out
	})
}

func FuzzSchemaChange(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseSchemaChange(&buf)
		dummySchC = out
	})
}
