package request

import (
	"scylla-go-driver/frame"
)


type Prepare struct {
	Query string
}


func (p Prepare) WriteTo(b *frame.Buffer) {
	b.WriteLongString(p.Query)
}
