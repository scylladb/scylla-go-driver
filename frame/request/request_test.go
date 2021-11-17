package request

import (
	"bytes"
	"fmt"
	"testing"
)

func bytesEqual(a , b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, _ := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// ------------------------------- AUTH RESPONSE TESTS --------------------------------

func TestAuthResponseWriteTo(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected []byte
	}{
		{"Should encode and decode",
			[]byte{0xca, 0xfe, 0xba, 0xbe},
			[]byte{0xca, 0xfe, 0xba, 0xbe},
		},

	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("AuthResponse Test %s", tc.name), func(t *testing.T) {
			ar := AuthResponse{tc.content}
			out := new(bytes.Buffer)
			ar.WriteTo(out)

			if bytesEqual(out.Bytes(), tc.expected) {
				t.Fatal("Failure while constructing 'Unavailable' error.")
			}
		})
	}
}