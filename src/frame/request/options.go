package request

import (
	"io"
	"scylla-go-driver/src/frame"
)

type Options struct {
	frame.Header
	// Options request doesn't have a body.
}

func NewOptions(ver byte, flags byte, StreamID uint16) *Options {
	o := Options{}
	o.Version = ver
	o.Opcode = frame.OpOptions
	o.Flags = flags
	o.StreamID = StreamID
	o.Length = 0 // Empty body.
	return &o
}

func (o *Options) WriteTo(writer io.Writer) (int64, error) {
	l, err := o.Header.WriteHeader(writer)
	if err != nil {
		return l, err
	}
	return l, nil
}
