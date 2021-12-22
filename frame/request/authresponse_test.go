package request

import (
	"github.com/google/go-cmp/cmp"
	"scylla-go-driver/frame"
	"testing"
)

func TestAuthResponseWriteTo(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected []byte
	}{
		{"Should encode and decode",
			[]byte{0xca, 0xfe, 0xba, 0xbe},
			[]byte{0x00, 0x00, 0x00, 0x04, 0xca, 0xfe, 0xba, 0xbe},
		},
	}
	t.Parallel()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ar := AuthResponse{Token: tc.content}
			var out frame.Buffer
			ar.WriteTo(&out)
			if diff := cmp.Diff(out.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
