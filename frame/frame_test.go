package frame

import (
	"bytes"
	"fmt"
	"testing"
)

func TestWriteByte(t *testing.T) {
	var cases = []struct {
		name     string
		nr       Byte
		expected []byte
	}{
		{"min byte", 0, []byte{0x0}},
		{"min positive byte", 1, []byte{0x01}},
		{"random big byte", 173, []byte{0xad}},
		{"max byte", 255, []byte{0xff}},
	}

	var buf bytes.Buffer
	for _, tc := range cases {

		t.Run(fmt.Sprintf("Bytes writing test %s", tc.name), func(t *testing.T) {
			WriteByte(tc.nr, &buf)

			if !bytes.Equal(buf.Bytes(), tc.expected) {
				t.Fatal("Failure while writing byte to buffer.")
			}
		})

		buf.Reset()
	}
}

func TestWriteShort(t *testing.T) {
	var cases = []struct {
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

	var buf bytes.Buffer
	for _, tc := range cases {

		t.Run(fmt.Sprintf("Short writing test %s", tc.name), func(t *testing.T) {
			WriteShort(tc.nr, &buf)

			if !bytes.Equal(buf.Bytes(), tc.expected) {
				t.Fatal("Failure while writing unsigned short to buffer.")
			}
		})

		buf.Reset()
	}
}

func TestWriteInt(t *testing.T) {
	var cases = []struct {
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

	var buf bytes.Buffer
	for _, tc := range cases {

		t.Run(fmt.Sprintf("Integer writing test %s", tc.name), func(t *testing.T) {
			WriteInt(tc.nr, &buf)

			if !bytes.Equal(buf.Bytes(), tc.expected) {
				t.Fatal("Failure while writing integer to buffer.")
			}
		})

		buf.Reset()
	}
}

func TestWriteString(t *testing.T) {
	var cases = []struct {
		name     string
		content  string
		expected []byte
	}{
		{"one char", "a", []byte{0x00, 0x01, 0x61}},
		{"normal word", "golang", []byte{0x00, 0x06, 0x67, 0x6f, 0x6c, 0x61, 0x6e, 0x67}},
		{"UTF-8 characters", "πœę©ß", []byte{0x00, 0x0a, 0xcf, 0x80, 0xc5, 0x93, 0xc4, 0x99, 0xc2, 0xa9, 0xc3, 0x9f}},
		{"empty string", "", []byte{0x00, 0x00}},
	}

	var buf bytes.Buffer
	for _, tc := range cases {

		t.Run(fmt.Sprintf("Integer writing test %s", tc.name), func(t *testing.T) {
			WriteString(tc.content, &buf)

			if !bytes.Equal(buf.Bytes(), tc.expected) {
				t.Fatal("Failure while writing integer to buffer.")
			}
		})

		buf.Reset()
	}
}
