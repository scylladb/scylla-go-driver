package transport

import (
	"scylla-go-driver/frame"
	. "scylla-go-driver/frame/request"
	. "scylla-go-driver/frame/response"
)

type Statement struct {
	content           string
	values            []frame.Value
	pageSize          frame.Int
	consistency       frame.Consistency
	serialConsistency frame.Consistency
	tracing           bool
	compression       bool
}

func NewStatement(content string) Statement {
	return Statement{
		content: content,
	}
}

func makeQueryForStatement(s Statement, pagingState frame.Bytes) Query {
	return Query{
		Query:       s.content,
		Consistency: s.consistency,
		Options: frame.QueryOptions{
			Values:            s.values,
			SerialConsistency: s.serialConsistency,
			PagingState:       pagingState,
			PageSize:          s.pageSize,
		},
	}
}

func newStatementFromCQL(cql string) Statement {
	return Statement{
		Content:     cql,
		Consistency: frame.ONE,
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
	case *VoidResult, *SchemaChangeResult, *SetKeyspaceResult:
		return QueryResult{}, nil
	default:
		return QueryResult{}, responseAsError(res)
	}
}
