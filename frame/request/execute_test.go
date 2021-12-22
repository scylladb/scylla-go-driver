package request

import (
	"scylla-go-driver/frame"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestExecuteWriteTo(t *testing.T) {
	var testCases = []struct {
		name     string
		content  Execute
		expected []byte
	}{
		{"Smoke encode",
			Execute{ID: frame.Bytes{0x01, 0x02}},
			[]byte{0x00, 0x02, 0x01, 0x02, 0x00},
		},
	}
	t.Parallel()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var out frame.Buffer
			tc.content.WriteTo(&out)
			if diff := cmp.Diff(out.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
