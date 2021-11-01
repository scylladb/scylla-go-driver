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
func NewOptions(ver byte, flags byte, streamID uint16) *Options {
	o := new(Options)
	o.head = &frame.Header{
		Version:  ver,
		Flags:    flags,
		StreamID: streamID,
		Opcode:   frame.OpOptions,
		Length:   0,
	}

	return o
}

func (o *Options) WriteTo(w io.Writer) (int64, error) {
	return o.head.Write(w)
}
