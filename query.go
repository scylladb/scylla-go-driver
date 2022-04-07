package scylla

import (
	"github.com/mmatczuk/scylla-go-driver/frame"
	"github.com/mmatczuk/scylla-go-driver/transport"
)

type Query struct {
	session *Session
	stmt    transport.Statement
	exec    func(*transport.Conn, transport.Statement, frame.Bytes) (transport.QueryResult, error)
}

func (q *Query) Exec() (Result, error) {
	conn := q.session.leastBusyConn()
	if conn == nil {
		return Result{}, errNoConnection
	}

	res, err := q.exec(conn, q.stmt, nil)
	return Result(res), err
}

func (q *Query) BindInt64(pos int, v int64) *Query {
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

	return q
}

type Result transport.QueryResult
