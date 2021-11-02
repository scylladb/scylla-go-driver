package request

import (
	"io"
	"scylla-go-driver/src/frame"
)

type Options struct {
	head *frame.Header
	// Options request doesn't have a body.
}

// NewOptions FIXME: arguments to this depend on context I don't know yet
func NewOptions(head *frame.Header) *Options {
	o := new(Options)
	o.head = head
	return o
}

func (o *Options) WriteTo(w io.Writer) (int64, error) {
	buf := make([]byte, 0, 128)
	o.head.WriteTo(&buf)
	res, err := w.Write(buf)

	return int64(res), err
}
