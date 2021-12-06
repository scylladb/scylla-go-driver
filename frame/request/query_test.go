package request

import (
	"github.com/google/go-cmp/cmp"
	"scylla-go-driver/frame"
	"testing"
)

func TestQuery(t *testing.T) {
	var cases = []struct {
		name     string
		content  Query
		expected []byte
	}{
		{"SELECT... Consistency ONE",
			Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options:     frame.QueryOptions{Flags: 0},
			},
			frame.MassAppendBytes(frame.LongStringToBytes("select * from system.local"),
				frame.ShortToBytes(frame.ONE),
				frame.ByteToBytes(0))},
		{"SELECT... Consistency QUORUM",
			Query{
				Query:       "select * from system.local",
				Consistency: frame.QUORUM,
				Options:     frame.QueryOptions{Flags: 0},
			},
			frame.MassAppendBytes(frame.LongStringToBytes("select * from system.local"),
				frame.ShortToBytes(frame.QUORUM),
				frame.ByteToBytes(0))},
		{"SELECT... Consistency ONE, FLAG: Values",
			Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: frame.QueryOptions{Flags: 0x01, Values: []frame.Value{{
					N:     4,
					Bytes: frame.HexStringToBytes("cafebabe"),
				}}},
			},
			frame.MassAppendBytes(frame.LongStringToBytes("select * from system.local"),
				frame.ShortToBytes(frame.ONE),
				frame.ByteToBytes(0x01),
				frame.ShortToBytes(1),
				frame.ValueToBytes(frame.Value{N: 4, Bytes: frame.HexStringToBytes("cafebabe")}))},
		{"SELECT... Consistency ONE, FLAG: SkipMetadata, PageSize, WithPagingState, WithSerialConsistency, WithDefaultTimestamp",
			Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: frame.QueryOptions{Flags: 0x02 | 0x04 | 0x08 | 0x10 | 0x20,
					PageSize:          10,
					PagingState:       frame.HexStringToBytes("cafebabe"),
					SerialConsistency: frame.LOCAL_SERIAL,
					Timestamp:         42},
			},
			frame.MassAppendBytes(frame.LongStringToBytes("select * from system.local"),
				frame.ShortToBytes(frame.ONE),
				frame.ByteToBytes(0x02|0x04|0x08|0x10|0x20),
				frame.IntToBytes(10),
				frame.IntToBytes(4),
				frame.HexStringToBytes("cafebabe"),
				frame.ShortToBytes(frame.LOCAL_SERIAL),
				frame.LongToBytes(42))},
		{"SELECT... Consistency ONE, FLAG: Values, WithNamesForValues",
			Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: frame.QueryOptions{Flags: 0x01 | 0x40,
					Values: []frame.Value{{N: 4, Bytes: frame.HexStringToBytes("cafebabe")}},
					Names:  []string{"foo"}},
			},
			frame.MassAppendBytes(frame.LongStringToBytes("select * from system.local"),
				frame.ShortToBytes(frame.ONE),
				frame.ByteToBytes(0x01|0x40),
				frame.ShortToBytes(1),
				frame.StringToBytes("foo"),
				frame.ValueToBytes(frame.Value{N: 4, Bytes: frame.HexStringToBytes("cafebabe")}))},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			b := frame.Buffer{}
			v.content.WriteTo(&b)
			if diff := cmp.Diff(v.expected, b.Bytes()); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}