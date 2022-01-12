package response

import (
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestAuthChallenge(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected AuthChallenge
	}{
		{
			name:     "simple",
			content:  frame.MassAppendBytes(frame.IntToBytes(frame.Int(4)), frame.HexStringToBytes("cafebabe")),
			expected: AuthChallenge{frame.HexStringToBytes("cafebabe")},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			a := ParseAuthChallenge(&buf)
			if diff := cmp.Diff(a, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
