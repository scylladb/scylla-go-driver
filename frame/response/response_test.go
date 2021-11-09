package response

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"scylla-go-driver/frame"
	"testing"
)

// equalStringList checks equality between two StringLists,
// by writing function on our own we avoid reflect.DeepEqual function.
func equalStringList(a, b frame.StringList) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func TestWriteSupported(t *testing.T) {
	var cases = []struct {
		name     string
		header   frame.Header
		body     frame.StringMultiMap
		expected []byte
	}{
		{"smoke test",
			frame.Header{
				Version:  0x84,
				Flags:    0,
				StreamID: 0,
				Opcode:   frame.OpSupported,
				Length:   0,
			},
			frame.StringMultiMap{
				"GOLang": {
					"is", "super", "awesome!",
				},
				"Pets": {
					"cat", "dog",
				},
			},
			[]byte{0x84, 0x0, 0x0, 0x0, 0x6, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x2, 0x0, 0x6, 0x47, 0x4f, 0x4c, 0x61, 0x6e, 0x67,
				0x0, 0x3, 0x0, 0x2, 0x69, 0x73, 0x0, 0x5, 0x73, 0x75,
				0x70, 0x65, 0x72, 0x0, 0x8, 0x61, 0x77, 0x65, 0x73,
				0x6f, 0x6d, 0x65, 0x21, 0x0, 0x4, 0x50, 0x65, 0x74,
				0x73, 0x0, 0x2, 0x0, 0x3, 0x63, 0x61, 0x74, 0x0, 0x3,
				0x64, 0x6f, 0x67}},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("StringList reading test %s", tc.name), func(t *testing.T) {
			frame.WriteHeader(tc.header, &buf)
			frame.WriteStringMultiMap(tc.body, &buf)

			if !bytes.Equal(buf.Bytes(), tc.expected) {
				t.Fatal("Failure while writing request 'Supported'.")
			}
		})

		buf.Reset()
	}
}

// equalHeader checks equality between two headers by
// casting them to byte slices.
func equalHeader(a, b frame.Header) bool {
	var a_buf bytes.Buffer
	enc_a := gob.NewEncoder(&a_buf)
	_ = enc_a.Encode(a)

	var b_buf bytes.Buffer
	enc_b := gob.NewEncoder(&b_buf)
	_ = enc_b.Encode(b)

	return bytes.Equal(a_buf.Bytes(), b_buf.Bytes())
}

// equalStringMultiMap checks equality between two StringMultiMaps,
// by writing function on our own we avoid reflect.DeepEqual function.
func equalStringMultiMap(a, b frame.StringMultiMap) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if !equalStringList(v, b[i]) {
			return false
		}
	}
	return true
}

func TestReadSupported(t *testing.T) {
	var cases = []struct {
		name    string
		content []byte
		exp_h   frame.Header
		exp_b   frame.StringMultiMap
	}{
		{"smoke test",
			[]byte{0x84, 0x0, 0x0, 0x0, 0x6, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x2, 0x0, 0x6, 0x47, 0x4f, 0x4c, 0x61, 0x6e, 0x67,
				0x0, 0x3, 0x0, 0x2, 0x69, 0x73, 0x0, 0x5, 0x73, 0x75,
				0x70, 0x65, 0x72, 0x0, 0x8, 0x61, 0x77, 0x65, 0x73,
				0x6f, 0x6d, 0x65, 0x21, 0x0, 0x4, 0x50, 0x65, 0x74,
				0x73, 0x0, 0x2, 0x0, 0x3, 0x63, 0x61, 0x74, 0x0, 0x3,
				0x64, 0x6f, 0x67},
			frame.Header{
				Version:  0x84,
				Flags:    0,
				StreamID: 0,
				Opcode:   frame.OpSupported,
				Length:   0,
			},
			frame.StringMultiMap{
				"GOLang": {
					"is", "super", "awesome!",
				},
				"Pets": {
					"cat", "dog",
				},
			}},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("StringList reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)

			h := frame.ReadHeader(&buf)
			if !equalHeader(h, tc.exp_h) {
				t.Fatal("Failure while reading request 'Supported', headers are not equal.")
			}

			smm := ReadSupported(&buf)
			if !equalStringMultiMap(smm.options, tc.exp_b) {
				t.Fatal("Failure while reading request 'Supported'.")
			}
		})

		buf.Reset()
	}
}
