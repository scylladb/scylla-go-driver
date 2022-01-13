package request

import (
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestPrepare(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  Prepare
		expected []byte
	}{
		{
			name:    "SELECT",
			content: Prepare{"SELECT * FROM foo"},
			expected: func() []byte {
				var b frame.Buffer
				b.WriteLongString("SELECT * FROM foo")
				return b.Bytes()
			}(),
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			tc.content.WriteTo(&buf)
			if diff := cmp.Diff(tc.expected, buf.Bytes()); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
