package request

import (
	"github.com/mmatczuk/scylla-go-driver/frame"
	"testing"
)

// We want to make sure that parsing does not crush driver even for random data.
func FuzzPrepare(f *testing.F) {
	f.Fuzz(func(t *testing.T, s string) { // nolint:thelper // This is not a helper function.
		in := Prepare{Query: s}
		var buf frame.Buffer
		in.WriteTo(&buf)
		if buf.Error() != nil {
			t.Error(buf.Error())
		}
	})
}
