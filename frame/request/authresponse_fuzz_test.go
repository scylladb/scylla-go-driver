package request

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

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
