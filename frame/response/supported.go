package response

import (
	"scylla-go-driver/frame"
)

type Supported struct {
	Options frame.StringMultiMap
}

func ParseSupported(b *frame.Buffer) (Supported, error) {
	return Supported{
		Options: b.ReadStringMultiMap(),
	}, b.Error()
}
