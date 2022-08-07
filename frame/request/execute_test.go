package request

import (
	"testing"

	"github.com/scylladb/scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestExecuteWriteTo(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  Execute
		expected []byte
	}{
		{
			name:     "Smoke encode",
			content:  Execute{ID: frame.Bytes{0x01, 0x02}, Consistency: frame.ONE},
			expected: []byte{0x00, 0x02, 0x01, 0x02, 0x00, 0x01, 0x00},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out frame.Buffer
			tc.content.WriteTo(&out)
			if diff := cmp.Diff(out.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
