package request

import (
	"io"
	"scylla-go-driver/src/frame"
)

type Options struct {
	frame.Header
	// Options request hasn't got a body.
}

//func NewOptions() *Options {
//	return &Options{}
//}

func NewOptions(ver byte, flags byte, streamId frame.Short) *Options {
	o := Options{}
	o.Version = ver
	o.Opcode = frame.OpOptions
	o.Flags = flags
	o.StreamId = streamId
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

