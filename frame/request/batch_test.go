package request

import (
	"fmt"
	"github.com/google/go-cmp/cmp"
	"math"
	"scylla-go-driver/frame"
	"testing"
)

func TestBatch(t *testing.T) {
	var cases = []struct {
		name    string
		content Batch
	}{
		{"Should encode and decode with v4.",
			Batch{Type: 0, Flags: 0,
				Queries:     []BatchQuery{{Kind: 0, Query: "SELECT * FROM foo"}},
				Consistency: 0x01, SerialConsistency: 0x08,
				Timestamp: frame.Long(math.MinInt64)}},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("Batch test %s", tc.name), func(t *testing.T) {
			var buf frame.Buffer
			tc.content.WriteTo(&buf)

			if batchType := buf.ReadByte(); batchType != tc.content.Type {
				t.Fatal("Invalid type.")
			}

			n := buf.ReadShort()
			if n != frame.Short(len(tc.content.Queries)) {
				t.Fatal("Invalid n.")
			}

			for i := frame.Short(0); i < n; i++ {
				if kind := buf.ReadByte(); kind == 0 {
					if que := buf.ReadLongString(); que != tc.content.Queries[i].Query {
						t.Fatal("Invalid query.")
					}
				} else if kind == 1 {
					prep := buf.ReadShortBytes()
					if diff := cmp.Diff(prep, tc.content.Queries[i].Prepared); diff != "" {
						t.Fatal(diff)
					}
				} else {
					t.Fatal("Invalid kind.")
				}

				values := buf.ReadShort()
				for j := frame.Short(0); j < values; j++ {
					if tc.content.Flags&WithNamesForValues != 0 {
						if name := buf.ReadString(); name != tc.content.Queries[i].Names[j] {
							t.Fatal("Invalid name.")
						}
					}
					val := buf.ReadValue()
					if diff := cmp.Diff(val, tc.content.Queries[i].Values[j]); diff != "" {
						t.Fatal(diff)
					}
				}
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
