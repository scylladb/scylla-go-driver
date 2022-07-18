package request

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

func FuzzExecute(f *testing.F) {
	f.Fuzz(func(t *testing.T, b1 byte, si1, si2 uint16, i1, i2 int32, li1 int64, s1, s2 string, bs1, bs2, bs3 []byte) {
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
