package response

import (
	"github.com/mmatczuk/scylla-go-driver/frame"
	"testing"
)

var dummyA *Authenticate

// We want to make sure that parsing does not crush driver even for random data.
// We assign result to global variable to avoid compiler optimization.
func FuzzAuthenticate(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseAuthenticate(&buf)
		dummyA = out
	})
}
