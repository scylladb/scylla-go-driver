package request

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestPrepare(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  Prepare
		expected []byte
	}{
		{
			name:    "SELECT",
			content: Prepare{"SELECT * FROM foo"},
			expected: func() []byte {
				var b frame.Buffer
				b.WriteLongString("SELECT * FROM foo")
				return b.Bytes()
			}(),
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := frame.Buffer{}
			tc.content.WriteTo(&b)
			if diff := cmp.Diff(tc.expected, b.Bytes()); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

// We want to make sure that parsing does not crush driver even for random data.
func FuzzPrepare(f *testing.F) {
	f.Fuzz(func(t *testing.T, s string) { // nolint:thelper // This is not a helper function.
		in := Prepare{Query: s}
		var buf frame.Buffer
		in.WriteTo(&buf)
		if buf.Error() != nil {
			t.Error(buf.Error())
		}
	})
}
