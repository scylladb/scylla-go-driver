package response

import (
	"github.com/google/go-cmp/cmp"
	"scylla-go-driver/frame"
	"testing"
)

func TestAuthChallenge(t *testing.T) {
	cases := []struct {
		name     string
		content  []byte
		expected AuthChallenge
	}{
		{"simple",
			frame.MassAppendBytes(frame.IntToBytes(frame.Int(4)), frame.HexStringToBytes("cafebabe")),
			AuthChallenge{frame.HexStringToBytes("cafebabe")}},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			var buf frame.Buffer
			buf.Write(v.content)
			a := ParseAuthChallenge(&buf)
			if diff := cmp.Diff(a, v.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
