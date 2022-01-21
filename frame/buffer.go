package frame

import (
	"bytes"
)

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
