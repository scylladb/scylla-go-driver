package transport

import (
	"fmt"

	"scylla-go-driver/frame"
	. "scylla-go-driver/frame/request"
	. "scylla-go-driver/frame/response"
)

type Statement struct {
	Content           string
	ID                frame.Bytes
	Values            []frame.Value
	PageSize          frame.Int
	Consistency       frame.Consistency
	SerialConsistency frame.Consistency
	Tracing           bool
	Compression       bool
	ColSpec           []frame.ColumnSpec
}

func BindString(s *Statement, t string, pos int) {
	s.Values[pos] = frame.Value{
		N:     frame.Int(len(t)),
		Bytes: []byte(t),
	}
}

type Integer interface {
	int | int8 | int16 | int32 |
	int64 | uint | uint8 | uint16 |
	uint32 | uint64
}

func BindNumber[T Integer](s *Statement, n T, pos int) {
	switch s.ColSpec[pos].Type.ID {
	case frame.BigintID | frame.CounterID:
		s.Values[pos] = frame.Value{
			N: 8,
			Bytes: []byte{
				byte(n >> 56),
				byte(n >> 48),
				byte(n >> 40),
				byte(n >> 32),
				byte(n >> 24),
				byte(n >> 16),
				byte(n >> 8),
				byte(n)},
		}
	case frame.IntID:
		s.Values[pos] = frame.Value{
			N: 4,
			Bytes: []byte{
				byte(n >> 24),
				byte(n >> 16),
				byte(n >> 8),
				byte(n)},
		}
	case frame.SmallintID:
		s.Values[pos] = frame.Value{
			N: 2,
			Bytes: []byte{
				byte(n >> 8),
				byte(n)},
		}
	case frame.TinyintID:
		s.Values[pos] = frame.Value{
			N:     1,
			Bytes: []byte{byte(n)},
		}
	default:
		return
	}
}

func newQueryForStatement(s Statement, pagingState frame.Bytes) frame.Request {
	return &Query{
		Query:       s.Content,
		Consistency: s.Consistency,
		Options: frame.QueryOptions{
			Values:            s.Values,
			SerialConsistency: s.SerialConsistency,
			PagingState:       pagingState,
			PageSize:          s.PageSize,
		},
	}
}

func newExecuteForStatement(s Statement, pagingState frame.Bytes) frame.Request {
	return &Execute{
		ID:          s.ID,
		Consistency: s.Consistency,
		Options: frame.QueryOptions{
			Values:            s.Values,
			SerialConsistency: s.SerialConsistency,
			PagingState:       pagingState,
			PageSize:          s.PageSize,
		},
	}
}

type QueryResult struct {
	Rows        []frame.Row
	Warnings    []string
	TracingID   frame.UUID
	PagingState frame.Bytes
	ColSpec     []frame.ColumnSpec
}

func makeQueryResult(res frame.Response) (QueryResult, error) {
	switch v := res.(type) {
	case *RowsResult:
		return QueryResult{
			Rows:        v.RowsContent,
			PagingState: v.Metadata.PagingState,
			ColSpec:     v.Metadata.Columns,
		}, nil
	case *VoidResult, *SchemaChangeResult:
		return QueryResult{}, nil
	default:
		return QueryResult{}, responseAsError(res)
	}
}

func makePrepareResult(res frame.Response) (Statement, error) {
	switch v := res.(type) {
	case *PreparedResult:
		return Statement{
			ID:      v.ID,
			Values:  make([]frame.Value, len(v.Metadata.Columns)),
			ColSpec: v.Metadata.Columns,
		}, nil
	case *Error:
		return Statement{}, fmt.Errorf("make prepare result: %s", v.Message)
	default:
		return Statement{}, fmt.Errorf("make prepare result: invalid result type %T, %+v", v, v)
	}
}
