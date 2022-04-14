package response

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestAuthSuccessEncodeDecode(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected []byte
	}{
		{
			name:     "Should encode and decode",
			content:  []byte{0x00, 0x00, 0x00, 0x04, 0xca, 0xfe, 0xba, 0xbe},
			expected: []byte{0xca, 0xfe, 0xba, 0xbe},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out frame.Buffer
			out.Write(tc.content)
			a := ParseAuthSuccess(&out)
			if diff := cmp.Diff(a.Token, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

var dummyAS *AuthSuccess

// We want to make sure that parsing does not crush driver even for random data.
// We assign result to global variable to avoid compiler optimization.
func FuzzAuthSuccess(f *testing.F) {
	testCases := [][]byte{make([]byte, 1000000)}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseAuthSuccess(&buf)
		dummyAS = out
	})
}
