package response

import (
	"github.com/google/go-cmp/cmp"
	"scylla-go-driver/frame"
	"testing"
)

func TestSupportedEncodeDecode(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected Supported
	}{
		{"Smoke test",
			[]byte{0x00, 0x01, 0x00, 0x01, 0x61, 0x00, 0x02, 0x00, 0x01, 0x61, 0x00, 0x01, 0x62},
			Supported{
				Options: frame.StringMultiMap{"a": {"a", "b"}},
			},
		},
	}
	t.Parallel()
	var out frame.Buffer
	for _, tc := range cases {
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
