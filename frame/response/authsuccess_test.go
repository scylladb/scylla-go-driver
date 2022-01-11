package response

import (
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestAuthSuccessEncodeDecode(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected []byte
	}{
		{
			"Should encode and decode",
			[]byte{0x00, 0x00, 0x00, 0x04, 0xca, 0xfe, 0xba, 0xbe},
			[]byte{0xca, 0xfe, 0xba, 0xbe},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out frame.Buffer
			out.Write(tc.content)
			a := ParseAuthSuccess(&out)
			if diff := cmp.Diff(a.Token, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
