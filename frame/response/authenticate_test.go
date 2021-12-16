package response

import (
	"github.com/google/go-cmp/cmp"
	"scylla-go-driver/frame"
	"testing"
)

func TestAuthenticateEncodeDecode(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected string
	}{
		{"Mock authenticator",
			[]byte{0x00, 0x11, 0x4d, 0x6f, 0x63, 0x6b, 0x41, 0x75, 0x74, 0x68, 0x65, 0x6e, 0x74, 0x69, 0x63, 0x61, 0x74, 0x6f, 0x72},
			"MockAuthenticator",
		},
	}

	var out frame.Buffer
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out.Write(tc.content)
			a := ParseAuthenticate(&out)
			if diff := cmp.Diff(a.Name, tc.expected); diff != "" {
				t.Fatal(diff)
			}
			if len(out.Bytes()) != 0 {
				t.Fatal("Failure buffer not empty after read.")
			}
		})
	}
}
