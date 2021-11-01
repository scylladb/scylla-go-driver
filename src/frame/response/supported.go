package response

import (
	"scylla-go-driver/src/frame"
)

type Supported struct {
	head    *frame.Header
	options frame.StringMultiMap
}

func NewSupported(head *frame.Header, buf *[]byte) (*Supported, error) {
	o := new(Supported)
	o.head = head
	if err := frame.ReadStringMultiMap(buf, o.options); err != nil {
		return o, err
	}

	return o, nil
}
