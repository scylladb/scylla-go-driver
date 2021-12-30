package transport

import (
	"fmt"
	"scylla-go-driver/frame"
	"scylla-go-driver/frame/response"
)

type Session struct {
	defaultConsistency frame.Consistency
	connections        *Connection
}

type QueryResult struct {
	Rows [][][]byte
	Err  error
}

func MakeSession(addr string) (Session, error) {
	conn, err := NewConnection(addr)
	return Session{
		defaultConsistency: frame.ONE,
		connections:        conn,
	}, err
}

type ResponseResult struct {
	receiver chan ConnResponse
}

func (r ResponseResult) Await() QueryResult {
	res := <-r.receiver

	buf := frame.NewBuffer(res.Body)

	switch res.Header.Opcode {
	case frame.OpError:
		err := response.ParseError(&buf)
		fmt.Println(err.Message)
		return QueryResult{Err: fmt.Errorf(err.Message)}
	case frame.OpResult:
		if n := buf.ReadInt(); n == 2 {
			rows := response.ParseRowsResult(&buf)
			return QueryResult{Rows: rows.RowsContent, Err: buf.Error()}
		}
		return QueryResult{Err: fmt.Errorf("unimplemented result")}
	default:
		return QueryResult{Err: fmt.Errorf("unimplemented OpCode")}
	}
}

func (s *Session) Query(query string) ResponseResult {
	responseReceiver := s.connections.Query(query)

	return ResponseResult{receiver: responseReceiver}
}
