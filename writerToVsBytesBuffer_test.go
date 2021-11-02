package frame

/*
import (
	"bytes"
	"io"
	"testing"
)

// go test -bench=.
// -benchtime=10s

func myWriteTo(n int, writer io.Writer) (int64, error) {
	buf := make([]byte, n)
	write, err := writer.Write(buf)
	if err != nil {
		return 0, err
	}
	return int64(write), nil
}

func myBytesBuffer(n int, bBuffer *bytes.Buffer) (int64, error) {
	buf := make([]byte, n)
	write, err := bBuffer.Write(buf)
	if err != nil {
		return 0, err
	}
	return int64(write), nil
}

var n = 10000

func BenchmarkWriterTo(b *testing.B) {
	var res int64
	buf := bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		to, err := myWriteTo(n, &buf)
		if err != nil {
			panic(err)
		}
		res += to
	}
}

func BenchmarkBytesBuffer(b *testing.B) {
	var res int64
	buf := bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		to, err := myBytesBuffer(n, &buf)
		if err != nil {
			panic(err)
		}
		res += to
	}
}*/
