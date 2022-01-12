package response

import (
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestSupportedEncodeDecode(t *testing.T) {
	t.Parallel()
	testCases := []struct {
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
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out frame.Buffer
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
