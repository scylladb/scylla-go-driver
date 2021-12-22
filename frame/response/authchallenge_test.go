package response

import (
	"scylla-go-driver/frame"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestAuthChallenge(t *testing.T) {
	testCases := []struct {
		name     string
		content  []byte
		expected AuthChallenge
	}{
		{"simple",
			frame.MassAppendBytes(frame.IntToBytes(frame.Int(4)), frame.HexStringToBytes("cafebabe")),
			AuthChallenge{frame.HexStringToBytes("cafebabe")}},
	}
	t.Parallel()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf frame.Buffer
			buf.Write(tc.content)
			a := ParseAuthChallenge(&buf)
			if diff := cmp.Diff(a, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
