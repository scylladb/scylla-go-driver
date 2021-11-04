package request

import (
	"bytes"
	"scylla-go-driver/frame"
)

type Options struct {
	head frame.Header
}

func NewOptions(h frame.Header) Options {
	return Options{h}
}

func WriteOptions(o Options, b *bytes.Buffer) {
	frame.WriteHeader(o.head, b)
}
