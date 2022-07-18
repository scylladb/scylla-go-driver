package request

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

func FuzzBatch(f *testing.F) {
	f.Fuzz(func(t *testing.T, b1, b2, b3 byte, si1, si2 uint16, i1, i2 int32, li1 int64, s1, s2, s3 string, bs1, bs2, bs3 []byte) {
		x := BatchQuery{
			Kind:     b1,
			Query:    s1,
			Prepared: bs1,
			Names:    frame.StringList{s2, s3},
			Values:   []frame.Value{{N: i1, Bytes: bs2}, {N: i2, Bytes: bs3}},
		}
		in := Batch{
			Type:              b2,
			Flags:             b3,
			Queries:           []BatchQuery{x, x},
			Consistency:       si1,
			SerialConsistency: si2,
			Timestamp:         li1,
		}
		var buf frame.Buffer
		in.WriteTo(&buf)
		if buf.Error() != nil {
			t.Error(buf.Error())
		}
	})
}
