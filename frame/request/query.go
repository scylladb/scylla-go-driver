package request

import (
	"scylla-go-driver/frame"
)

type Query struct {
	Query       string
	Consistency frame.Consistency
	Options     frame.QueryOptions
}

func (q Query) WriteTo(b *frame.Buffer) {
	b.WriteLongString(q.Query)
	b.WriteConsistency(q.Consistency)
	b.WriteQueryOptions(q.Options)
}

