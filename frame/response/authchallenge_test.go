package response

import (
	"encoding/hex"
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func hexStringToBytes(s string) []byte {
	tmp, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return tmp
}

func writeHexStringTo(b *frame.Buffer, s string) {
	for _, v := range hexStringToBytes(s) {
		b.WriteByte(v)
	}
}
func TestAuthChallenge(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected AuthChallenge
	}{
		{
			name: "simple",
			content: func() []byte {
				var b frame.Buffer
				b.WriteInt(frame.Int(4))
				writeHexStringTo(&b, "cafebabe")
				return b.Bytes()
			}(),
			expected: AuthChallenge{hexStringToBytes("cafebabe")},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			a := ParseAuthChallenge(&buf)
			if diff := cmp.Diff(*a, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
