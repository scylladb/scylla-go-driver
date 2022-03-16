package transport

import (
	"scylla-go-driver/frame"
	. "scylla-go-driver/frame/request"
	. "scylla-go-driver/frame/response"
)

type Statement struct {
	Content           string
	Values            []frame.Value
	PageSize          frame.Int
	Consistency       frame.Consistency
	SerialConsistency frame.Consistency
	Tracing           bool
	Compression       bool
}

func makeQueryForStatement(s Statement, pagingState frame.Bytes) Query {
	return Query{
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
