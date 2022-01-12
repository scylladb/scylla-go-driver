package response

import (
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestAuthenticateEncodeDecode(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected string
	}{
		{
			name:     "Mock authenticator",
			content:  []byte{0x00, 0x11, 0x4d, 0x6f, 0x63, 0x6b, 0x41, 0x75, 0x74, 0x68, 0x65, 0x6e, 0x74, 0x69, 0x63, 0x61, 0x74, 0x6f, 0x72},
			expected: "MockAuthenticator",
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out frame.Buffer
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
