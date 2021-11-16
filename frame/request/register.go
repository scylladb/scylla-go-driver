package request

import (
	"bytes"
	"scylla-go-driver/frame"
)

type Register struct {
	eventTypes frame.StringList
}

func (r Register) WriteTo(b *bytes.Buffer) {
	frame.WriteStringList(r.eventTypes, b)
}
