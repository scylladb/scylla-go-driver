package request

import (
	"testing"

	"github.com/scylladb/scylla-go-driver/frame"
)

func FuzzStartup(f *testing.F) {
	f.Fuzz(func(t *testing.T, s1, s2, s3, s4, s5, s6 string) {
		in := Startup{Options: frame.StartupOptions{s1: s2, s3: s4, s5: s6}}
		var buf frame.Buffer
		in.WriteTo(&buf)
		if buf.Error() != nil {
			t.Error(buf.Error())
		}
	})
}
