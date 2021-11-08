package response

import (
	"bytes"
	"scylla-go-driver/frame"
)

type Supported struct {
	head    frame.Header
	options frame.StringMultiMap
}

func ReadSupported(h frame.Header, b *bytes.Buffer) Supported {
	m := frame.ReadStringMultiMap(b)
	return Supported{h, m}
}
