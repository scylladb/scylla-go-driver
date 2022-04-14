package request

import (
	"math"
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func readQuery(t *testing.T, buf *frame.Buffer, exp *BatchQuery, withNamesForValues bool) {
	t.Helper()
	switch buf.ReadByte() {
	case 0:
		if que := buf.ReadLongString(); que != exp.Query {
			t.Fatal("Invalid query.")
		}

	case 1:
		prep := buf.ReadShortBytes()
		if diff := cmp.Diff(prep, exp.Prepared); diff != "" {
			t.Fatal(diff)
		}

	default:
		t.Fatal("Invalid kind.")
	}

	values := buf.ReadShort()
	for j := frame.Short(0); j < values; j++ {
		if withNamesForValues {
			if name := buf.ReadString(); name != exp.Names[j] {
				t.Fatal("Invalid name.")
			}
		}
		val := buf.ReadValue()
		if diff := cmp.Diff(val, exp.Values[j]); diff != "" {
			t.Fatal(diff)
		}
	}
}

func TestBatch(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name    string
		content Batch
	}{
		{
			name: "Should encode and decode with v4.",
			content: Batch{
				Type: 0, Flags: 0,
				Queries:     []BatchQuery{{Kind: 0, Query: "SELECT * FROM foo"}},
				Consistency: 0x01, SerialConsistency: 0x08,
				Timestamp: frame.Long(math.MinInt64),
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			tc.content.WriteTo(&buf)
			if batchType := buf.ReadByte(); batchType != tc.content.Type {
				t.Fatal("Invalid type.")
			}

			n := buf.ReadShort()
			if n != frame.Short(len(tc.content.Queries)) {
				t.Fatal("Invalid n.")
			}

			for j := frame.Short(0); j < n; j++ {
				readQuery(t, &buf, &tc.content.Queries[j], tc.content.Flags&WithNamesForValues != 0)
			}

			if cons := buf.ReadShort(); cons != tc.content.Consistency {
				t.Fatal("Invalid consistency.")
			}

			flag := buf.ReadByte()
			if flag != tc.content.Flags {
				t.Fatal("Invalid flag.")
			}

			if flag&frame.WithSerialConsistency != 0 {
				if serCons := buf.ReadShort(); serCons != tc.content.SerialConsistency {
					t.Fatal("Invalid serial consistency.")
				}
			}

			if flag&frame.WithDefaultTimestamp != 0 {
				if time := buf.ReadLong(); time != tc.content.Timestamp {
					t.Fatal("Invalid time.")
				}
			}
		})
	}
}

// We want to make sure that parsing does not crush driver even for random data.
func FuzzBatch(f *testing.F) {
	f.Fuzz(func(t *testing.T, b1, b2, b3 byte, si1, si2 uint16, i1, i2 int32, li1 int64, s1, s2, s3 string, bs1, bs2, bs3 []byte) { // nolint:thelper // This is not a helper function.
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
