package response

import (
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestAuthChallenge(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		content  []byte
		expected AuthChallenge
	}{
		{
			"simple",
			frame.MassAppendBytes(frame.IntToBytes(frame.Int(4)), frame.HexStringToBytes("cafebabe")),
			AuthChallenge{frame.HexStringToBytes("cafebabe")},
		},
	}
	for i := 0; i < len(cases); i++ {
		v := cases[i]
		t.Run(v.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(v.content)
			a := ParseAuthChallenge(&buf)
			if diff := cmp.Diff(a, v.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
