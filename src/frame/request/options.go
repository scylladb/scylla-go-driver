package request

import (
	"io"
	"scylla-go-driver/src/frame"
)

type Options struct {
	frame.Header
	// Options request has not a body.
}

func NewOptions() *Options {
	return &Options{}
}

func (o *Options) WriteTo(writer io.Writer) (int64, error) {
	// Unimplemented!
	return 0, nil
}

