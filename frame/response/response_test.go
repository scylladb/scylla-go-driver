package response

import (
	"bytes"
	"scylla-go-driver/frame"
	"testing"
)

func EqualStringList(a, b frame.StringList) bool {
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

func TestSimpleSupported(t *testing.T) {
	b := &bytes.Buffer{}
	m := frame.StringMultiMap{
		"GOLang": {
			"is", "super", "awesome!",
		},
		"Pets": {
			"cat", "dog",
		},
	}
	h := frame.Header{
		Version:  0x84,
		Flags:    0,
		StreamID: 0,
		Opcode:   frame.OpSupported,
		Length:   0,
	}
	frame.WriteHeader(h, b)
	frame.WriteStringMultiMap(m, b)
	h2 := frame.ReadHeader(b)
	s := ReadSupported(h2, b)
	if s.head != h {
		t.Errorf("header")
	}
	if !EqualStringList(m["GOLang"], s.options["GOLang"]) {
		t.Errorf("GOlang")
	}
	if !EqualStringList(m["Pets"], s.options["Pets"]) {
		t.Errorf("Pets")
	}
}
