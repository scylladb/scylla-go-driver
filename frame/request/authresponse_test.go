package request

import (
	"scylla-go-driver/frame"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestAuthResponseWriteTo(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected []byte
	}{
		{
			name:     "Should encode and decode",
			content:  []byte{0xca, 0xfe, 0xba, 0xbe},
			expected: []byte{0x00, 0x00, 0x00, 0x04, 0xca, 0xfe, 0xba, 0xbe},
		},
	}
	t.Parallel()
	for _, tc := range testCases {
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
