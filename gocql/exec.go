package gocql

import (
	"context"
	"fmt"

	"github.com/scylladb/scylla-go-driver"
	"github.com/scylladb/scylla-go-driver/frame"
	"github.com/scylladb/scylla-go-driver/transport"
)

// SingleHostQueryExecutor allows to quickly execute diagnostic queries while
// connected to only a single node.
// The executor opens only a single connection to a node and does not use
// connection pools.
// Consistency level used is ONE.
// Retry policy is applied, attempts are visible in query metrics but query
// observer is not notified.
type SingleHostQueryExecutor struct {
	conn *transport.Conn
}

func bind(stmt *transport.Statement, values []interface{}) error {
	if len(stmt.Values) != len(values) {
		return fmt.Errorf("bind: expected %d columns, got %d", len(stmt.Values), len(values))
	}

	for i := range values {
		v := anyWrapper{values[i]}
		var err error
		stmt.Values[i].N, stmt.Values[i].Bytes, err = v.Serialize(&stmt.Prepared.Metadata.Columns[i].Type)
		if err != nil {
			return err
		}
	}

	return nil
}

// Exec executes the query without returning any rows.
func (e SingleHostQueryExecutor) Exec(stmt string, values ...interface{}) error {
	qStmt := transport.Statement{Content: stmt, Consistency: frame.ONE}
	qStmt, err := e.conn.Prepare(context.Background(), qStmt)
	if err != nil {
		return err
	}

	if err := bind(&qStmt, values); err != nil {
		return err
	}
	_, err = e.conn.Query(context.Background(), qStmt, nil)
	return err
}

// Iter executes the query and returns an iterator capable of iterating
// over all results.
func (e SingleHostQueryExecutor) Iter(stmt string, values ...interface{}) *Iter {
	qStmt := transport.Statement{Content: stmt, Consistency: frame.ONE}
	qStmt, err := e.conn.Prepare(context.Background(), qStmt)
	if err == nil {
		err = bind(&qStmt, values)
	}
	it := newIter(newSingleHostIter(qStmt, e.conn))
	it.err = err
	return it
}

func (e SingleHostQueryExecutor) Close() {
	if e.conn != nil {
		e.conn.Close()
	}
}

// NewSingleHostQueryExecutor creates a SingleHostQueryExecutor by connecting
// to one of the hosts specified in the ClusterConfig.
// If ProtoVersion is not specified version 4 is used.
// Caller is responsible for closing the executor after use.
func NewSingleHostQueryExecutor(cfg *ClusterConfig) (e SingleHostQueryExecutor, err error) {
	if len(cfg.Hosts) < 1 {
		return
	}

	var scfg scylla.SessionConfig
	scfg, err = sessionConfigFromGocql(cfg)
	if err != nil {
		return
	}

	host := cfg.Hosts[0]
	var control *transport.Conn
	control, err = transport.OpenConn(context.Background(), host, nil, scfg.ConnConfig)
	if err != nil {
		if control != nil {
			control.Close()
		}
		return
	}
	e = SingleHostQueryExecutor{control}
	return
}

type singleHostIter struct {
	conn   *transport.Conn
	result transport.QueryResult
	pos    int
	rowCnt int
	closed bool
	err    error
	rd     transport.RetryDecider
	stmt   transport.Statement
}

func newSingleHostIter(stmt transport.Statement, conn *transport.Conn) *singleHostIter {
	return &singleHostIter{
		conn:   conn,
		stmt:   stmt,
		rd:     &transport.DefaultRetryDecider{},
		result: transport.QueryResult{HasMorePages: true},
	}
}

func (it *singleHostIter) fetch() (transport.QueryResult, error) {
	if !it.result.HasMorePages {
		return transport.QueryResult{}, scylla.ErrNoMoreRows
	}
	for {
		res, err := it.conn.Query(context.Background(), it.stmt, it.result.PagingState)
		if err == nil {
			return res, nil
		} else if err != nil {
			ri := transport.RetryInfo{
				Error:       err,
				Idempotent:  it.stmt.Idempotent,
				Consistency: 1,
			}
			if it.rd.Decide(ri) != transport.RetrySameNode {
				return transport.QueryResult{}, err
			}
		}
	}
}

func (it *singleHostIter) Next() (frame.Row, error) {
	if it.closed {
		return nil, it.err
	}

	if it.pos >= it.rowCnt {
		var err error
		it.result, err = it.fetch()
		if err != nil {
			it.err = dropNoMoreRows(err)
			return nil, it.Close()
		}

		it.pos = 0
		it.rowCnt = len(it.result.Rows)
	}

	// We probably got a zero-sized last page, retry to be sure
	if it.rowCnt == 0 {
		return it.Next()
	}

	res := it.result.Rows[it.pos]
	it.pos++
	return res, nil
}

func (it *singleHostIter) Close() error {
	if it.closed {
		return dropNoMoreRows(it.err)
	}
	it.closed = true
	return it.err
}

func (it *singleHostIter) Columns() []frame.ColumnSpec {
	return it.stmt.Prepared.ResultMetadata.Columns
}

func (it *singleHostIter) NumRows() int {
	return it.rowCnt
}

func (it *singleHostIter) PageState() []byte {
	return it.result.PagingState
}
