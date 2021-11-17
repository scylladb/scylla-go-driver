package request

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"scylla-go-driver/frame"
	"testing"
)

func bytesEqual(a , b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, _ := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func ShortToBytes(x frame.Short) []byte {
	var out bytes.Buffer
	frame.WriteShort(x, &out)
	return out.Bytes()
}

func IntToBytes(x frame.Int) []byte {
	var out bytes.Buffer
	frame.WriteInt(x, &out)
	return out.Bytes()
}

func StringToBytes(x string) []byte {
	var out bytes.Buffer
	frame.WriteString(x, &out)
	return out.Bytes()
}

func LongStringToBytes(x string) []byte {
	var out bytes.Buffer
	frame.WriteLongString(x, &out)
	return out.Bytes()
}

func ByteToBytes(b frame.Byte) []byte {
	var out bytes.Buffer
	frame.WriteByte(b, &out)
	return out.Bytes()
}

func massAppendBytes(elems ...[]byte) []byte {
	var ans []byte
	for _, v := range elems {
		ans = append(ans, v...)
	}
	return ans
}

func StringListToBytes(sl frame.StringList) []byte {
	var out bytes.Buffer
	frame.WriteStringList(sl, &out)
	return out.Bytes()
}

func BytesToBytes(b frame.Bytes) []byte {
	var out bytes.Buffer
	frame.WriteBytes(b, &out)
	return out.Bytes()
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
			[]byte{0xca, 0xfe, 0xba, 0xbe},
		},

	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("AuthResponse Test %s", tc.name), func(t *testing.T) {
			ar := AuthResponse{tc.content}
			out := new(bytes.Buffer)
			ar.WriteTo(out)

			if bytesEqual(out.Bytes(), tc.expected) {
				t.Fatal("Failure while encoding and decoding AuthResponse.")
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
		t.Run("TestPrepare: "+v.name+".", func(t *testing.T) {
			b := bytes.Buffer{}
			v.content.Write(&b)
			if !bytes.Equal(v.expected, b.Bytes()) {
				t.Fatal("Writing Prepare request to buffer failed.")
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
	b := bytes.Buffer{}
	frame.WriteValue(v, &b)
	return b.Bytes()
}

func LongToBytes(l frame.Long) []byte {
	b := bytes.Buffer{}
	frame.WriteLong(l, &b)
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
				Options:     QueryOptions{Flags: 0},
			},
			massAppendBytes(LongStringToBytes("select * from system.local"),
				ShortToBytes(frame.ONE),
				ByteToBytes(0))},
		{"SELECT... Consistency QUORUM",
			Query{
				Query:       "select * from system.local",
				Consistency: frame.QUORUM,
				Options:     QueryOptions{Flags: 0},
			},
			massAppendBytes(LongStringToBytes("select * from system.local"),
				ShortToBytes(frame.QUORUM),
				ByteToBytes(0))},
		{"SELECT... Consistency ONE, FLAG: values",
			Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: QueryOptions{Flags: 0x01, Values: []frame.Value{{
					4,
					HexStringToBytes("cafebabe"),
				}}},
			},
			massAppendBytes(LongStringToBytes("select * from system.local"),
				ShortToBytes(frame.ONE),
				ByteToBytes(0x01),
				ShortToBytes(1),
				ValueToBytes(frame.Value{N: 4, Bytes: HexStringToBytes("cafebabe")}))},
		{"SELECT... Consistency ONE, FLAG: skipMetadata, pageSize, pagingState, serialConsistency, timestamp",
			Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: QueryOptions{Flags: 0x02 | 0x04 | 0x08 | 0x10 | 0x20,
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
		{"SELECT... Consistency ONE, FLAG: values, namedValues",
			Query{
				Query:       "select * from system.local",
				Consistency: frame.ONE,
				Options: QueryOptions{Flags: 0x01 | 0x40,
					Values: []frame.Value{{4, HexStringToBytes("cafebabe")}},
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
		t.Run("TestQuery: "+v.name+".", func(t *testing.T) {
			b := bytes.Buffer{}
			v.content.Write(&b)
			if !bytes.Equal(v.expected, b.Bytes()) {
				t.Fatal("Writing Query request to buffer failed.")
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
			[]byte{0x0f, 0x00, 0x54, 0x4f, 0x50, 0x4f, 0x4c, 0x4f, 0x47, 0x59, 0x5f,  0x43,
							0x48, 0x41, 0x4e, 0x47, 0x45, 0x0d, 0x00, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f,
							0x43, 0x48, 0x41, 0x4e, 0x47, 0x45, 0x0d, 0x00, 0x53, 0x43, 0x48, 0x45, 0x4d, 0x41,
							0x5f, 0x43, 0x48, 0x41, 0x4e, 0x47, 0x45},
		},

	}

	var out bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("AuthResponse Test %s", tc.name), func(t *testing.T) {
			r := Register{tc.content}
			r.WriteTo(&out)

			if bytesEqual(out.Bytes(), tc.expected) {
				t.Fatal("Failure while encoding and decoding AuthResponse.")
			}
		})

		out.Reset()
	}
}

// ------------------------------- STARTUP TESTS -----------------------------

func StringMapEqual(a, b frame.StringMap) bool {
	for k, v := range a {
		if mv, ok := b[k]; !(ok && mv == v) {
			return false
		}
	}
	for k, v := range b {
		if mv, ok := a[k]; !(ok && mv == v) {
			return false
		}
	}
	return true
}

func TestWriteStartup(t *testing.T) {
	var cases = []struct {
		name     string
		content  Startup
	}{
		{"mandatory only",
			Startup{
				options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
				},
			},
		},
		{"compression",
			Startup{
				options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"COMPRESSION": "lz4",
				},
			},
		},
		{"custom option",
			Startup{
				options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"CUSTOM_OPT1": "VALUE1",
				},
			},
		},
		{"custom option + compression",
			Startup{
				options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"CUSTOM_OPT1": "VALUE1",
					"COMPRESSION": "lz4",
				},
			},
		},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Short writing test %s", tc.name), func(t *testing.T) {
			tc.content.Write(&buf)
			readOptions := frame.ReadStringMap(&buf)

			if !StringMapEqual(readOptions, tc.content.options) {
				t.Fatal("Failure while constructing Startup.")
			}
		})

		buf.Reset()
	}
}

