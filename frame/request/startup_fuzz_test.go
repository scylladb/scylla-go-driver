package request

import (
	"github.com/mmatczuk/scylla-go-driver/frame"
	"testing"
)

// We want to make sure that parsing does not crush driver even for random data.
func FuzzStartup(f *testing.F) {
	f.Fuzz(func(t *testing.T, s1, s2, s3, s4, s5, s6 string) { // nolint:thelper // This is not a helper function.
		in := Startup{Options: frame.StartupOptions{s1: s2, s3: s4, s5: s6}}
		var buf frame.Buffer
		in.WriteTo(&buf)
		if buf.Error() != nil {
			t.Error(buf.Error())
		}
	})
}
