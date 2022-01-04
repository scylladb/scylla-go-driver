package frame

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestWriteByte(t *testing.T) {
	var testCases = []struct {
		name     string
		nr       Byte
		expected []byte
	}{
		{"min byte", 0, []byte{0x0}},
		{"min positive byte", 1, []byte{0x01}},
		{"random big byte", 173, []byte{0xad}},
		{"max byte", 255, []byte{0xff}},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.WriteByte(tc.nr)
			if diff := cmp.Diff(buf.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
		buf.Reset()
	}
}

func TestWriteShort(t *testing.T) {
	var testCases = []struct {
		name     string
		nr       Short
		expected []byte
	}{
		{"min short", 0, []byte{0x0, 0x0}},
		{"max byte", 255, []byte{0x0, 0xff}},
		{"min non byte", 256, []byte{0x01, 0x00}},
		{"random big short", 7919, []byte{0x1e, 0xef}},
		{"max short", 65535, []byte{0xff, 0xff}},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.WriteShort(tc.nr)
			if diff := cmp.Diff(buf.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
		buf.Reset()
	}
}

func TestWriteInt(t *testing.T) {
	var testCases = []struct {
		name     string
		nr       Int
		expected []byte
	}{
		{"min integer", -2147483648, []byte{0x80, 0x0, 0x0, 0x0}},
		{"zero", 0, []byte{0x0, 0x0, 0x0, 0x0}},
		{"min positive integer", 1, []byte{0x0, 0x0, 0x0, 0x01}},
		{"random short", 9452, []byte{0x0, 0x0, 0x24, 0xec}},
		{"random 3 byte numer", 123335, []byte{0x0, 0x01, 0xe1, 0xc7}},
		{"max integer", 2147483647, []byte{0x7f, 0xff, 0xff, 0xff}},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.WriteInt(tc.nr)
			if diff := cmp.Diff(buf.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
		buf.Reset()
	}
}

func TestWriteString(t *testing.T) {
	var testCases = []struct {
		name     string
		content  string
		expected []byte
	}{
		{"one char", "a", []byte{0x00, 0x01, 0x61}},
		{"normal word", "golang", []byte{0x00, 0x06, 0x67, 0x6f, 0x6c, 0x61, 0x6e, 0x67}},
		{"UTF-8 characters", "πœę©ß", []byte{0x00, 0x0a, 0xcf, 0x80, 0xc5, 0x93, 0xc4, 0x99, 0xc2, 0xa9, 0xc3, 0x9f}},
		{"empty string", "", []byte{0x00, 0x00}},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.WriteString(tc.content)
			if diff := cmp.Diff(buf.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
		buf.Reset()
	}
}

func TestWriteStringList(t *testing.T) {
	var testCases = []struct {
		name     string
		content  StringList
		expected []byte
	}{
		{"one string", StringList{"a"}, []byte{0x00, 0x01, 0x00, 0x01, 0x61}},
		{"two strings", StringList{"a", "b"}, []byte{0x00, 0x02, 0x00, 0x01, 0x61, 0x00, 0x01, 0x62}},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.WriteStringList(tc.content)
			if diff := cmp.Diff(buf.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
		buf.Reset()
	}
}

func TestWriteStringMultiMap(t *testing.T) {
	var testCases = []struct {
		name     string
		content  StringMultiMap
		expected []byte
	}{
		{"Smoke test", StringMultiMap{"a": {"a"}}, []byte{0x00, 0x01, 0x00, 0x01, 0x61, 0x00, 0x01, 0x00, 0x01, 0x61}},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.WriteStringMultiMap(tc.content)
			if diff := cmp.Diff(buf.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
		buf.Reset()
	}
}

func TestWriteHeader(t *testing.T) {
	var testCases = []struct {
		name     string
		content  Header
		expected []byte
	}{
		{
			name: "plain supported",
			content: Header{
				Version:  CQLv4,
				Flags:    0,
				StreamID: 0,
				Opcode:   OpSupported,
				Length:   0,
			},
			expected: []byte{0x84, 0x0, 0x0, 0x0, 0x06, 0x0, 0x0, 0x0, 0x0},
		},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.content.WriteTo(&buf)
			if diff := cmp.Diff(buf.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
		buf.Reset()
	}
}

func TestReadByte(t *testing.T) {
	var testCases = []struct {
		name     string
		nr       []byte
		expected Byte
	}{
		{"min byte", []byte{0x00}, 0},
		{"random small byte", []byte{0x16}, 22},
		{"random large byte", []byte{0x7d}, 125},
		{"max byte", []byte{0xff}, 255},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.nr)
			out := buf.ReadByte()
			if out != tc.expected {
				t.Fatal("Failure while reading Byte.")
			}
		})
		buf.Reset()
	}
}

func TestReadShort(t *testing.T) {
	var testCases = []struct {
		name     string
		nr       []byte
		expected Short
	}{
		{"min short", []byte{0x00, 0x00}, 0},
		{"random small short", []byte{0x00, 0xf5}, 245},
		{"random large short", []byte{0xa7, 0xf3}, 42995},
		{"max short", []byte{0xff, 0xff}, 65535},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.nr)
			out := buf.ReadShort()
			if out != tc.expected {
				t.Fatal("Failure while reading Short.")
			}
		})
		buf.Reset()
	}
}

func TestReadInt(t *testing.T) {
	var testCases = []struct {
		name     string
		nr       []byte
		expected Int
	}{
		{"min integer", []byte{0x80, 0x0, 0x0, 0x0}, -2147483648},
		{"zero", []byte{0x0, 0x0, 0x0, 0x0}, 0},
		{"min positive integer", []byte{0x0, 0x0, 0x0, 0x01}, 1},
		{"random short", []byte{0x0, 0x0, 0x24, 0xec}, 9452},
		{"random 3 byte numer", []byte{0x0, 0x01, 0xe1, 0xc7}, 123335},
		{"max integer", []byte{0x7f, 0xff, 0xff, 0xff}, 2147483647},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.nr)
			out := buf.ReadInt()
			if out != tc.expected {
				t.Fatal("Failure while reading Integer.")
			}
		})
		buf.Reset()
	}
}

func TestReadString(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected string
	}{
		{"one char", []byte{0x00, 0x01, 0x61}, "a"},
		{"normal word", []byte{0x00, 0x06, 0x67, 0x6f, 0x6c, 0x61, 0x6e, 0x67}, "golang"},
		{"UTF-8 characters", []byte{0x00, 0x0a, 0xcf, 0x80, 0xc5, 0x93, 0xc4, 0x99, 0xc2, 0xa9, 0xc3, 0x9f}, "πœę©ß"},
		{"empty string", []byte{0x00, 0x00}, ""},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out := buf.ReadString()
			if out != tc.expected {
				t.Fatal("Failure while writing reading String.")
			}
		})
		buf.Reset()
	}
}

func TestReadStringList(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected StringList
	}{
		{"one string", []byte{0x00, 0x01, 0x00, 0x01, 0x61}, StringList{"a"}},
		{"two strings", []byte{0x00, 0x02, 0x00, 0x01, 0x61, 0x00, 0x01, 0x62}, StringList{"a", "b"}},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out := buf.ReadStringList()
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
		buf.Reset()
	}
}

func TestReadStringMultiMap(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected StringMultiMap
	}{
		{"Smoke test", []byte{0x00, 0x01, 0x00, 0x01, 0x61, 0x00, 0x01, 0x00, 0x01, 0x61}, StringMultiMap{"a": {"a"}}},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out := buf.ReadStringMultiMap()
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
		buf.Reset()
	}
}

func TestReadHeader(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected Header
	}{
		{
			name:    "plain supported",
			content: []byte{0x84, 0x0, 0x0, 0x0, 0x06, 0x0, 0x0, 0x0, 0x0},
			expected: Header{
				Version:  CQLv4,
				Flags:    0,
				StreamID: 0,
				Opcode:   OpSupported,
				Length:   0,
			},
		},
	}

	var buf Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out := ParseHeader(&buf)
			if out != tc.expected {
				t.Fatal("Failure while reading StringMultiMap.")
			}
		})
		buf.Reset()
	}
}
