package response

import (
	"errors"
	"scylla-go-driver/src/frame"
)

type Supported struct {
	head    *frame.Header
	options frame.StringMultiMap
}

func NewSupported(head *frame.Header, buf *[]byte) (*Supported, error) {
	s := new(Supported)
	s.head = head
	inBuf := len(*buf)
	if err := frame.ReadStringMultiMap(buf, s.options); err != nil {
		return nil, err
	}
	if len(*buf) + int(s.head.Length) != inBuf {
		return nil, errors.New("header length != read bytes")
	}

	return s, nil
}
