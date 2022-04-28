package request

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

// We want to make sure that parsing does not crush driver even for random data.
func FuzzQuery(f *testing.F) {
	f.Fuzz(func(t *testing.T, b1 byte, si1, si2 uint16, i1, i2 int32, li1 int64, s1, s2, s3 string, bs1, bs2 []byte) {
		in := Query{
			Query:       s1,
			Consistency: si1,
			Options: frame.QueryOptions{
				Flags:             b1,
				Values:            []frame.Value{{N: i1, Bytes: bs1}},
				Names:             frame.StringList{s2, s3},
				PageSize:          i2,
				PagingState:       bs2,
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
