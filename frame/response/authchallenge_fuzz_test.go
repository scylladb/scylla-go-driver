package response

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

var dummyAC *AuthChallenge

// We assign the result to a global variable to avoid compiler optimization.
func FuzzAuthChallenge(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseAuthChallenge(&buf)
		dummyAC = out
	})
}
