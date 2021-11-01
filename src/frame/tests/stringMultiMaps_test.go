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
	tmpBuf := buf.Bytes()
	err = frame.ReadStringMultiMap(&tmpBuf, m2)

	if err != nil {
		panic(err)
	} else if len(tmpBuf) != 0 {
		panic("Buffer should be empty.")
	}
}
