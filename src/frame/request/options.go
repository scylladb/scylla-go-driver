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
	o := new(Options)
	return o
}

func (o *Options) WriteTo(writer io.Writer) (int64, error) {
	l, err := o.Header.WriteHeader(writer)
	if err != nil {
		return l, err
	}
	return l, nil
}

