package frame

import (
	"bytes"
	"testing"
)

func Equal(a, b []string) bool {
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
	b := bytes.Buffer{}
	err := error(nil)

	w := bufWrapper{&b, &err}

	m := StringMultiMap{
		"GOLang": {
			"is", "super", "awesome!",
		},
		"Pets": {
			"cat", "dog",
		},
	}

	h := Header{
		Version:  0x84,
		Flags:    0,
		StreamID: 0,
		Opcode:   OpSupported,
		Length:   0,
	}

	w.WriteHeader(h)
	w.WriteStringMultiMap(m)

	h2 := w.ReadHeader()
	s := w.ReadSupported(h2)

	if s.head != h {
		t.Errorf("header")
	}

	if !Equal(m["GOLang"], s.options["GOLang"]) {
		t.Errorf("GOlang")
	}

	if !Equal(m["Pets"], s.options["Pets"]) {
		t.Errorf("Pets")
	}
}
