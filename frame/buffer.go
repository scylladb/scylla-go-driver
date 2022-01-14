package frame

import (
	"bytes"
	"io"
)

// CopyBuffer writes data to w until the buffer is drained or an error occurs.
func CopyBuffer(b *Buffer, w io.Writer) (n int64, err error) {
	return b.buf.WriteTo(w)
}

type Buffer struct {
	buf     bytes.Buffer
	readErr error
}

func (b *Buffer) Bytes() []byte {
	return b.buf.Bytes()
}

func (b *Buffer) Reset() {
	b.buf.Reset()
}
