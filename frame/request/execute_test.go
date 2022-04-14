package request

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestExecuteWriteTo(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  Execute
		expected []byte
	}{
		{
			name:     "Smoke encode",
			content:  Execute{ID: frame.Bytes{0x01, 0x02}, Consistency: frame.ONE},
			expected: []byte{0x00, 0x02, 0x01, 0x02, 0x00, 0x01, 0x00},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out frame.Buffer
			tc.content.WriteTo(&out)
			if diff := cmp.Diff(out.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

// We want to make sure that parsing does not crush driver even for random data.
func FuzzExecute(f *testing.F) {
	f.Fuzz(func(t *testing.T, b1 byte, si1, si2 uint16, i1, i2 int32, li1 int64, s1, s2 string, bs1, bs2, bs3 []byte) { // nolint:thelper // This is not a helper function.
		in := Execute{
			ID:          bs1,
			Consistency: si1,
			Options: frame.QueryOptions{
				Flags:             b1,
				Values:            []frame.Value{{N: i1, Bytes: bs2}},
				Names:             frame.StringList{s1, s2},
				PageSize:          i2,
				PagingState:       bs3,
				SerialConsistency: si2,
				Timestamp:         li1,
			},
		}
		var buf frame.Buffer
		in.WriteTo(&buf)
		if buf.Error() != nil {
			t.Error(buf.Error())
		}
	})
}
