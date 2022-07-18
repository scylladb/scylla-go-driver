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
				t.Fatal("invalid type")
			}

			n := buf.ReadShort()
			if n != frame.Short(len(tc.content.Queries)) {
				t.Fatal("invalid n")
			}

			for j := frame.Short(0); j < n; j++ {
				readQuery(t, &buf, &tc.content.Queries[j], tc.content.Flags&WithNamesForValues != 0)
			}

			if cons := buf.ReadShort(); cons != tc.content.Consistency {
				t.Fatal("invalid consistency")
			}

			flag := buf.ReadByte()
			if flag != tc.content.Flags {
				t.Fatal("invalid flag")
			}

			if flag&frame.WithSerialConsistency != 0 {
				if serCons := buf.ReadShort(); serCons != tc.content.SerialConsistency {
					t.Fatal("invalid serial consistency")
				}
			}

			if flag&frame.WithDefaultTimestamp != 0 {
				if time := buf.ReadLong(); time != tc.content.Timestamp {
					t.Fatal("invalid time")
				}
			}
		})
	}
}
