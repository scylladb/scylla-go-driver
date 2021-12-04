package request

import (
	"encoding/hex"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"math"
	"scylla-go-driver/frame"
	"testing"
)

func ShortToBytes(x frame.Short) []byte {
	var out frame.Buffer
	out.WriteShort(x)
	return out.Bytes()
}

func IntToBytes(x frame.Int) []byte {
	var out frame.Buffer
	out.WriteInt(x)
	return out.Bytes()
}

func StringToBytes(x string) []byte {
	var out frame.Buffer
	out.WriteString(x)
	return out.Bytes()
}

func LongStringToBytes(x string) []byte {
	var out frame.Buffer
	out.WriteLongString(x)
	return out.Bytes()
}

func ByteToBytes(b frame.Byte) []byte {
	var out frame.Buffer
	out.WriteByte(b)
	return out.Bytes()
}

func massAppendBytes(elems ...[]byte) []byte {
	var ans []byte
	for _, v := range elems {
		ans = append(ans, v...)
	}
	return ans
}

// ------------------------------- AUTH RESPONSE TESTS --------------------------------

func TestAuthResponseWriteTo(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected []byte
	}{
		{"Should encode and decode",
			[]byte{0xca, 0xfe, 0xba, 0xbe},
			[]byte{0x00, 0x00, 0x00, 0x04, 0xca, 0xfe, 0xba, 0xbe},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ar := AuthResponse{Token: tc.content}
			var out frame.Buffer
			ar.WriteTo(&out)
			if diff := cmp.Diff(out.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

// ------------------------------- PREPARE TESTS --------------------------------

func TestPrepare(t *testing.T) {
	var cases = []struct {
		name     string
		content  Prepare
		expected []byte
	}{
		{"SELECT", Prepare{"SELECT * FROM foo"}, LongStringToBytes("SELECT * FROM foo")},
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

// ------------------------------- QUERY TESTS --------------------------------

// HexStringToBytes does begin with string's length.
func HexStringToBytes(s string) []byte {
	tmp, _ := hex.DecodeString(s)
	return tmp
}

func ValueToBytes(v frame.Value) []byte {
	b := frame.Buffer{}
	b.WriteValue(v)
	return b.Bytes()
}

func LongToBytes(l frame.Long) []byte {
	b := frame.Buffer{}
	b.WriteLong(l)
	return b.Bytes()
}

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
			massAppendBytes(LongStringToBytes("select * from system.local"),
				ShortToBytes(frame.ONE),
				ByteToBytes(0))},
		{"SELECT... Consistency QUORUM",
			Query{
				Query:       "select * from system.local",
				Consistency: frame.QUORUM,
				Options:     frame.QueryOptions{Flags: 0},
			},
			massAppendBytes(LongStringToBytes("select * from system.local"),
				ShortToBytes(frame.QUORUM),
				ByteToBytes(0))},
		{"SELECT... Consistency ONE, FLAG: Values",
			Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: frame.QueryOptions{Flags: 0x01, Values: []frame.Value{{
					N:     4,
					Bytes: HexStringToBytes("cafebabe"),
				}}},
			},
			massAppendBytes(LongStringToBytes("select * from system.local"),
				ShortToBytes(frame.ONE),
				ByteToBytes(0x01),
				ShortToBytes(1),
				ValueToBytes(frame.Value{N: 4, Bytes: HexStringToBytes("cafebabe")}))},
		{"SELECT... Consistency ONE, FLAG: SkipMetadata, PageSize, WithPagingState, WithSerialConsistency, WithDefaultTimestamp",
			Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: frame.QueryOptions{Flags: 0x02 | 0x04 | 0x08 | 0x10 | 0x20,
					PageSize:          10,
					PagingState:       HexStringToBytes("cafebabe"),
					SerialConsistency: frame.LOCAL_SERIAL,
					Timestamp:         42},
			},
			massAppendBytes(LongStringToBytes("select * from system.local"),
				ShortToBytes(frame.ONE),
				ByteToBytes(0x02|0x04|0x08|0x10|0x20),
				IntToBytes(10),
				IntToBytes(4),
				HexStringToBytes("cafebabe"),
				ShortToBytes(frame.LOCAL_SERIAL),
				LongToBytes(42))},
		{"SELECT... Consistency ONE, FLAG: Values, WithNamesForValues",
			Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: frame.QueryOptions{Flags: 0x01 | 0x40,
					Values: []frame.Value{{N: 4, Bytes: HexStringToBytes("cafebabe")}},
					Names:  []string{"foo"}},
			},
			massAppendBytes(LongStringToBytes("select * from system.local"),
				ShortToBytes(frame.ONE),
				ByteToBytes(0x01|0x40),
				ShortToBytes(1),
				StringToBytes("foo"),
				ValueToBytes(frame.Value{N: 4, Bytes: HexStringToBytes("cafebabe")}))},
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

// ------------------------------- REGISTER TESTS --------------------------------

func TestRegister(t *testing.T) {
	var cases = []struct {
		name     string
		content  frame.StringList
		expected []byte
	}{
		{"Should encode and decode",
			frame.StringList{"TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE"},
			[]byte{0x0f, 0x00, 0x54, 0x4f, 0x50, 0x4f, 0x4c, 0x4f, 0x47, 0x59, 0x5f, 0x43,
				0x48, 0x41, 0x4e, 0x47, 0x45, 0x0d, 0x00, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f,
				0x43, 0x48, 0x41, 0x4e, 0x47, 0x45, 0x0d, 0x00, 0x53, 0x43, 0x48, 0x45, 0x4d, 0x41,
				0x5f, 0x43, 0x48, 0x41, 0x4e, 0x47, 0x45},
		},
	}

	var out frame.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("AuthResponse Test %s", tc.name), func(t *testing.T) {
			r := Register{tc.content}
			r.WriteTo(&out)
			if diff := cmp.Diff(out.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
		out.Reset()
	}
}

// ------------------------------- STARTUP TESTS -----------------------------

func TestWriteStartup(t *testing.T) {
	var cases = []struct {
		name    string
		content Startup
	}{
		{"mandatory only",
			Startup{
				Options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
				},
			},
		},
		{"compression",
			Startup{
				Options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"COMPRESSION": "lz4",
				},
			},
		},
		{"custom option",
			Startup{
				Options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"CUSTOM_OPT1": "VALUE1",
				},
			},
		},
		{"custom option + compression",
			Startup{
				Options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"CUSTOM_OPT1": "VALUE1",
					"COMPRESSION": "lz4",
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("Short writing test %s", tc.name), func(t *testing.T) {
			var buf frame.Buffer
			tc.content.WriteTo(&buf)
			readOptions := buf.ReadStringMap()
			if diff := cmp.Diff(readOptions, tc.content.Options); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

// ------------------------------- BATCH TESTS -----------------------------

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
