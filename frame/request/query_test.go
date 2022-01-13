package request

import (
	"encoding/hex"
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func hexStringToBytes(s string) []byte {
	tmp, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return tmp
}

func writeHexStringTo(b *frame.Buffer, s string) {
	for _, v := range hexStringToBytes(s) {
		b.WriteByte(v)
	}
}

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
			expected: func() []byte {
				var b frame.Buffer
				b.WriteLongString("select * from system.local")
				b.WriteShort(frame.ONE)
				b.WriteByte(0)
				return b.Bytes()
			}(),
		},
		{
			name: "SELECT... Consistency QUORUM",
			content: Query{
				Query:       "select * from system.local",
				Consistency: frame.QUORUM,
				Options:     frame.QueryOptions{Flags: 0},
			},
			expected: func() []byte {
				var b frame.Buffer
				b.WriteLongString("select * from system.local")
				b.WriteShort(frame.QUORUM)
				b.WriteByte(0)
				return b.Bytes()
			}(),
		},
		{
			name: "SELECT... Consistency ONE, FLAG: Values",
			content: Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: frame.QueryOptions{Flags: 0x01, Values: []frame.Value{{
					N:     4,
					Bytes: hexStringToBytes("cafebabe"),
				}}},
			},
			expected: func() []byte {
				var b frame.Buffer
				b.WriteLongString("select * from system.local")
				b.WriteShort(frame.ONE)
				b.WriteByte(0x01)
				b.WriteShort(1)
				b.WriteValue(frame.Value{N: 4, Bytes: hexStringToBytes("cafebabe")})
				return b.Bytes()
			}(),
		},
		{
			name: "SELECT... Consistency ONE, FLAG: SkipMetadata, PageSize, WithPagingState, WithSerialConsistency, WithDefaultTimestamp",
			content: Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: frame.QueryOptions{
					Flags:             0x02 | 0x04 | 0x08 | 0x10 | 0x20,
					PageSize:          10,
					PagingState:       hexStringToBytes("cafebabe"),
					SerialConsistency: frame.LOCALSERIAL,
					Timestamp:         42,
				},
			},
			expected: func() []byte {
				var b frame.Buffer
				b.WriteLongString("select * from system.local")
				b.WriteShort(frame.ONE)
				b.WriteByte(0x02 | 0x04 | 0x08 | 0x10 | 0x20)
				b.WriteInt(10)
				b.WriteInt(4)
				writeHexStringTo(&b, "cafebabe")
				b.WriteShort(frame.LOCALSERIAL)
				b.WriteLong(42)
				return b.Bytes()
			}(),
		},
		{
			name: "SELECT... Consistency ONE, FLAG: Values, WithNamesForValues",
			content: Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: frame.QueryOptions{
					Flags:  0x01 | 0x40,
					Values: []frame.Value{{N: 4, Bytes: hexStringToBytes("cafebabe")}},
					Names:  []string{"foo"},
				},
			},
			expected: func() []byte {
				var b frame.Buffer
				b.WriteLongString("select * from system.local")
				b.WriteShort(frame.ONE)
				b.WriteByte(0x01 | 0x40)
				b.WriteShort(1)
				b.WriteString("foo")
				b.WriteValue(frame.Value{N: 4, Bytes: hexStringToBytes("cafebabe")})
				return b.Bytes()
			}(),
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
