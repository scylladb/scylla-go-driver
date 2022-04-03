package scylla

import (
	"github.com/mmatczuk/scylla-go-driver/frame"
	"github.com/mmatczuk/scylla-go-driver/transport"
)

type Query struct {
	stmt transport.Statement
}

func (q *Query) BindInt64(pos int, v int64) {
	q.stmt.Values[pos] = frame.Value{
		N: 8,
		Bytes: []byte{
			byte(v >> 56),
			byte(v >> 48),
			byte(v >> 40),
			byte(v >> 32),
			byte(v >> 24),
			byte(v >> 16),
			byte(v >> 8),
			byte(v),
		},
	}
}

type Result transport.QueryResult
