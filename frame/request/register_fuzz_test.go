package request

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

// We want to make sure that parsing does not crush driver even for random data.
func FuzzRegister(f *testing.F) {
	f.Fuzz(func(t *testing.T, s1, s2, s3 string) {
		in := Register{EventTypes: []frame.EventType{s1, s2, s3}}
		var buf frame.Buffer
		in.WriteTo(&buf)
		if buf.Error() != nil {
			t.Error(buf.Error())
		}
	})
}
