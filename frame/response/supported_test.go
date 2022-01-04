package response

import (
	"scylla-go-driver/frame"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSupportedEncodeDecode(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected Supported
	}{
		{
			name:    "Smoke test",
			content: []byte{0x00, 0x01, 0x00, 0x01, 0x61, 0x00, 0x02, 0x00, 0x01, 0x61, 0x00, 0x01, 0x62},
			expected: Supported{
				Options: frame.StringMultiMap{"a": {"a", "b"}},
			},
		},
	}
	t.Parallel()
	var out frame.Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out.Write(tc.content)
			a := ParseSupported(&out)
			if diff := cmp.Diff(a, tc.expected); diff != "" {
				t.Fatal(diff)
			}
			if len(out.Bytes()) != 0 {
				t.Fatal("Failure buffer not empty after read.")
			}
		})
	}
}
