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
	tmp_buf := buf.Bytes()
	err = frame.ReadStringMultiMap(&tmp_buf, m2)

	if err != nil {
		panic(err)
	} else if len(tmp_buf) != 0 {
		panic("Buffer should be empty.")
	}
}
