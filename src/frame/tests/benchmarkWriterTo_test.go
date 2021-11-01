package tests

import (
	"bytes"
	"scylla-go-driver/src/frame"
	"scylla-go-driver/src/frame/request"
	"scylla-go-driver/src/frame/response"
	"testing"
)


// In this directory run go test -bench=.


func optionsWriterTo() (err error) {
	result := []byte{0x0, 0x0, 0x0, 0x1, 0x5, 0x0, 0x0, 0x0, 0x0}
	buf := bytes.Buffer{}
	options := request.NewOptions(0, 0, 1)
	_, err = options.WriteTo(&buf)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(result, buf.Bytes()) {
		panic("bytes not equal")
	}
	return
}

func BenchmarkOptionsWriterTo(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := optionsWriterTo()
		if err != nil {
			panic(err)
		}
	}
}


func supportedRead() (err error) {
	var m = frame.StringMultiMap{
		"GOLang": {
			"is", "super", "awesome!",
		},
		"Pets": {
			"cat", "dog",
		},
	}
	buf := bytes.Buffer{}
	h := frame.Header{}
	h.Version = 0b10000000 // Response
	h.Flags = 0
	h.StreamId = 1
	h.Opcode = frame.OpSupported
	h.Length = 51 // map size
	header, err := h.WriteHeader(&buf)
	if err != nil || header != 9 {
		panic(err)
	}
	wrote, err := frame.WriteStringMultiMap(m, &buf)
	if err != nil || int32(wrote) != h.Length {
		panic(err)
	}

	if len(buf.Bytes()) != int(h.Length) + int(header) {
		panic("invalid buf")
	}

	_, err = response.NewSupported(buf.Bytes())
	if err != nil {
		panic(err)
	}
	return
}

func BenchmarkSupportedRead(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := supportedRead()
		if err != nil {
			panic(err)
		}
	}
}

