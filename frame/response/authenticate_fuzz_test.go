package response

import (
	"testing"

	"github.com/scylladb/scylla-go-driver/frame"
)

var dummyA *Authenticate

// We assign the result to a global variable to avoid compiler optimization.
func FuzzAuthenticate(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseAuthenticate(&buf)
		dummyA = out
	})
}
