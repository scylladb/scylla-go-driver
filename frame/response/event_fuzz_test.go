package response

import (
	"github.com/mmatczuk/scylla-go-driver/frame"
	"testing"
)

var (
	dummyStaC *StatusChange
	dummyTopC *TopologyChange
	dummySchC *SchemaChange
)

// We want to make sure that parsing does not crush driver even for random data.
// We assign result to global variable to avoid compiler optimization.
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
