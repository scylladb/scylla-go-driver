package request

import (
	"bytes"
	"fmt"
	"scylla-go-driver/frame"
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
				t.Fatal("Failure while encoding and decoding AuthResponse.")
			}
		})
	}
}

// ------------------------------- REGISTER TESTS --------------------------------

func TestRegister(t *testing.T) {
	var cases = []struct {
		name     string
		content  frame.StringList
		expected []byte
	}{
		{"Should encode and decode",
			frame.StringList{"TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE"},
			[]byte{0x0f, 0x00, 0x54, 0x4f, 0x50, 0x4f, 0x4c, 0x4f, 0x47, 0x59, 0x5f,  0x43,
							0x48, 0x41, 0x4e, 0x47, 0x45, 0x0d, 0x00, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f,
							0x43, 0x48, 0x41, 0x4e, 0x47, 0x45, 0x0d, 0x00, 0x53, 0x43, 0x48, 0x45, 0x4d, 0x41,
							0x5f, 0x43, 0x48, 0x41, 0x4e, 0x47, 0x45},
		},

	}

	var out bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("AuthResponse Test %s", tc.name), func(t *testing.T) {
			r := Register{tc.content}
			r.WriteTo(&out)

			if bytesEqual(out.Bytes(), tc.expected) {
				t.Fatal("Failure while encoding and decoding AuthResponse.")
			}
		})

		out.Reset()
	}
}