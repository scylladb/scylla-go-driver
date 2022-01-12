package request

import (
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestQuery(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  Query
		expected []byte
	}{
		{
			name: "SELECT... Consistency ONE",
			content: Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options:     frame.QueryOptions{Flags: 0},
			},
			expected: frame.MassAppendBytes(frame.LongStringToBytes("select * from system.local"),
				frame.ShortToBytes(frame.ONE),
				frame.ByteToBytes(0)),
		},
		{
			name: "SELECT... Consistency QUORUM",
			content: Query{
				Query:       "select * from system.local",
				Consistency: frame.QUORUM,
				Options:     frame.QueryOptions{Flags: 0},
			},
			expected: frame.MassAppendBytes(frame.LongStringToBytes("select * from system.local"),
				frame.ShortToBytes(frame.QUORUM),
				frame.ByteToBytes(0)),
		},
		{
			name: "SELECT... Consistency ONE, FLAG: Values",
			content: Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: frame.QueryOptions{Flags: 0x01, Values: []frame.Value{{
					N:     4,
					Bytes: frame.HexStringToBytes("cafebabe"),
				}}},
			},
			expected: frame.MassAppendBytes(frame.LongStringToBytes("select * from system.local"),
				frame.ShortToBytes(frame.ONE),
				frame.ByteToBytes(0x01),
				frame.ShortToBytes(1),
				frame.ValueToBytes(frame.Value{N: 4, Bytes: frame.HexStringToBytes("cafebabe")})),
		},
		{
			name: "SELECT... Consistency ONE, FLAG: SkipMetadata, PageSize, WithPagingState, WithSerialConsistency, WithDefaultTimestamp",
			content: Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: frame.QueryOptions{
					Flags:             0x02 | 0x04 | 0x08 | 0x10 | 0x20,
					PageSize:          10,
					PagingState:       frame.HexStringToBytes("cafebabe"),
					SerialConsistency: frame.LOCALSERIAL,
					Timestamp:         42,
				},
			},
			expected: frame.MassAppendBytes(frame.LongStringToBytes("select * from system.local"),
				frame.ShortToBytes(frame.ONE),
				frame.ByteToBytes(0x02|0x04|0x08|0x10|0x20),
				frame.IntToBytes(10),
				frame.IntToBytes(4),
				frame.HexStringToBytes("cafebabe"),
				frame.ShortToBytes(frame.LOCALSERIAL),
				frame.LongToBytes(42)),
		},
		{
			name: "SELECT... Consistency ONE, FLAG: Values, WithNamesForValues",
			content: Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: frame.QueryOptions{
					Flags:  0x01 | 0x40,
					Values: []frame.Value{{N: 4, Bytes: frame.HexStringToBytes("cafebabe")}},
					Names:  []string{"foo"},
				},
			},
			expected: frame.MassAppendBytes(frame.LongStringToBytes("select * from system.local"),
				frame.ShortToBytes(frame.ONE),
				frame.ByteToBytes(0x01|0x40),
				frame.ShortToBytes(1),
				frame.StringToBytes("foo"),
				frame.ValueToBytes(frame.Value{N: 4, Bytes: frame.HexStringToBytes("cafebabe")})),
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			tc.content.WriteTo(&buf)
			if diff := cmp.Diff(tc.expected, buf.Bytes()); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
