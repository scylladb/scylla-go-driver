package frame

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestBufferReadByte(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		nr       []byte
		expected Byte
	}{
		{"min byte", []byte{0x00}, 0},
		{"random small byte", []byte{0x16}, 22},
		{"random large byte", []byte{0x7d}, 125},
		{"max byte", []byte{0xff}, 255},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf Buffer
			buf.Write(tc.nr)
			out := buf.ReadByte()
			if out != tc.expected {
				t.Fatal("Failure while reading Byte.")
			}
		})
	}
}

func TestBufferReadShort(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		nr       []byte
		expected Short
	}{
		{"min short", []byte{0x00, 0x00}, 0},
		{"random small short", []byte{0x00, 0xf5}, 245},
		{"random large short", []byte{0xa7, 0xf3}, 42995},
		{"max short", []byte{0xff, 0xff}, 65535},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf Buffer
			buf.Write(tc.nr)
			out := buf.ReadShort()
			if out != tc.expected {
				t.Fatal("Failure while reading Short.")
			}
		})
	}
}

func TestBufferReadInt(t *testing.T) {
	t.Parallel()
	testCases := []struct {
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
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf Buffer
			buf.Write(tc.nr)
			out := buf.ReadInt()
			if out != tc.expected {
				t.Fatal("Failure while reading Integer.")
			}
		})
	}
}

func TestBufferReadString(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected string
	}{
		{"one char", []byte{0x00, 0x01, 0x61}, "a"},
		{"normal word", []byte{0x00, 0x06, 0x67, 0x6f, 0x6c, 0x61, 0x6e, 0x67}, "golang"},
		{"UTF-8 characters", []byte{0x00, 0x0a, 0xcf, 0x80, 0xc5, 0x93, 0xc4, 0x99, 0xc2, 0xa9, 0xc3, 0x9f}, "πœę©ß"},
		{"empty string", []byte{0x00, 0x00}, ""},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf Buffer
			buf.Write(tc.content)
			out := buf.ReadString()
			if out != tc.expected {
				t.Fatal("Failure while writing reading String.")
			}
		})
	}
}

func TestBufferReadStringList(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected StringList
	}{
		{"one string", []byte{0x00, 0x01, 0x00, 0x01, 0x61}, StringList{"a"}},
		{"two strings", []byte{0x00, 0x02, 0x00, 0x01, 0x61, 0x00, 0x01, 0x62}, StringList{"a", "b"}},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf Buffer
			buf.Write(tc.content)
			out := buf.ReadStringList()
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestBufferReadStringMultiMap(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected StringMultiMap
	}{
		{"Smoke test", []byte{0x00, 0x01, 0x00, 0x01, 0x61, 0x00, 0x01, 0x00, 0x01, 0x61}, StringMultiMap{"a": {"a"}}},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf Buffer
			buf.Write(tc.content)
			out := buf.ReadStringMultiMap()
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestBuffer_ReadUUID(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected UUID
	}{
		{
			"Smoke test",
			[]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
			UUID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf Buffer
			buf.Write(tc.content)
			out := buf.ReadUUID()
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestBufferReadHeader(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected Header
	}{
		{
			name:    "plain supported",
			content: []byte{0x04, 0x0, 0x0, 0x0, 0x06, 0x0, 0x0, 0x0, 0x0},
			expected: Header{
				Version:  CQLv4,
				Flags:    0,
				StreamID: 0,
				OpCode:   OpSupported,
				Length:   0,
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf Buffer
			buf.Write(tc.content)
			out := ParseHeader(&buf)
			if out != tc.expected {
				t.Fatal("Failure while reading StringMultiMap.")
			}
		})
	}
}
