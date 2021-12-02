package request

import (
	"scylla-go-driver/frame"
)

type Execute struct {
	ID      frame.Bytes
	Options QueryOptions
}

func (e Execute) WriteTo(b *frame.Buffer) {
	b.WriteShortBytes(e.ID)
	e.Options.WriteTo(b) // actually we want to have b.WriteQueryOptions
}
