package request

import (
	"scylla-go-driver/frame"
)

type Register struct {
	EventTypes []frame.EventType
}

func (r Register) WriteTo(b *frame.Buffer) {
	b.WriteEventTypes(r.EventTypes)
}
