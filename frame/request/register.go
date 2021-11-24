package request

import (
	"scylla-go-driver/frame"
)

type Register struct {
	EventTypes frame.StringList
}

func (r Register) WriteTo(b *frame.Buffer) {
	b.WriteStringList(r.EventTypes)
}
