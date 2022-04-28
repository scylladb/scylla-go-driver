package request

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

// We want to make sure that parsing does not crush driver even for random data.
func FuzzAuthResponse(f *testing.F) {
	f.Add("", "")
	f.Add("user", "password")

	f.Fuzz(func(t *testing.T, user, password string) {
		in := AuthResponse{
			Username: user,
			Password: password,
		}
		var buf frame.Buffer
		in.WriteTo(&buf)
		if buf.Error() != nil {
			t.Error(buf.Error())
		}
	})
}
