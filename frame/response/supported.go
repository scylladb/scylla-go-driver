package response

import "scylla-go-driver/frame"

type Supported struct {
	head    frame.Header
	options frame.StringMultiMap
}

func ReadSupported(h frame.Header, b frame.Buffer) Supported {
	m := frame.ReadStringMultiMap(b)
	return Supported{h, m}
}
