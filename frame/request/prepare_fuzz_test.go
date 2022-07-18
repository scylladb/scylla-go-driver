package request

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

func FuzzPrepare(f *testing.F) {
	f.Fuzz(func(t *testing.T, s string) {
		in := Prepare{Query: s}
		var buf frame.Buffer
		in.WriteTo(&buf)
		if buf.Error() != nil {
			t.Error(buf.Error())
		}
	})
}
