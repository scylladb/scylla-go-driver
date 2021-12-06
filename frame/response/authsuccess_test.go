package response

import (
	"github.com/google/go-cmp/cmp"
	"scylla-go-driver/frame"
	"testing"
)

func TestAuthSuccessEncodeDecode(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected []byte
	}{
		{"Should encode and decode",
			[]byte{0x00, 0x00, 0x00, 0x04, 0xca, 0xfe, 0xba, 0xbe},
			[]byte{0xca, 0xfe, 0xba, 0xbe},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out frame.Buffer
			out.Write(tc.content)
			a := ParseAuthSuccess(&out)
			if diff := cmp.Diff(a.Token, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}