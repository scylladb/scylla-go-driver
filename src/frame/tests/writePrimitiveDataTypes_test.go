package tests

import (
	"bytes"
	"scylla-go-driver/src/frame"
	"testing"
)

func TestWriteShort(t *testing.T) {
	buf := new(bytes.Buffer)
	wrote, err := frame.WriteShort(5, buf)
	if err != nil || wrote != 2 {
		panic("Writing to buffer error")
	}
	res := []byte{0x0, 0x5}

	//TODO is https://pkg.go.dev/github.com/stretchr/testify/assert better for assertions?
	if !bytes.Equal(res, buf.Bytes()) {
		panic("WriteShort of 5 should return 0x0 and 0x5")
	}

	buf.Reset()
	wrote, err = frame.WriteShort(7919, buf)
	if err != nil || wrote != 2 {
		panic("Writing to buffer error")
	}

	res = []byte{0x1e, 0xef}
	if !bytes.Equal(res, buf.Bytes()) {
		panic("WriteShort of 7919 should return 0x1e and 0xef")
	}
}