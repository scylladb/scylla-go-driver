package tests

import (
	"bytes"
	"scylla-go-driver/src/frame"
	"testing"
)

func TestWriteReadStringMultiMap(t *testing.T) {
	var m = frame.StringMultiMap{
		"GOLang": {
			"is", "super", "awesome!",
		},
		"Pets": {
			"cat", "dog",
		},
	}
	buf := new(bytes.Buffer)
	_, err := frame.WriteStringMultiMap(m, buf)
	if err != nil {
		return
	}
	m2 := make(frame.StringMultiMap)
	err = frame.ReadStringMultiMap(buf.Bytes(), m2)
	// TODO: buf.Bytes() still contains all the bytes, which should've been drained.
	if err != nil {
		panic(err)
	}

}
