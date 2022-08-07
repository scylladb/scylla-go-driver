package response

import (
	"testing"

	"github.com/scylladb/scylla-go-driver/frame"
)

var dummyS *Supported

// We assign the result to a global variable to avoid compiler optimization.
func FuzzSupported(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseSupported(&buf)
		dummyS = out
	})
}
