package scylla

import (
	"fmt"

	"scylla-go-driver/frame"
	"scylla-go-driver/transport"
)

// TODO: Add retry policy.
// TODO: Add Query Paging.
// TODO: Merge with host selection policy.

type SessionConfig struct {
	Hosts    []string
	Keyspace string
	Events   []frame.EventType
	transport.ConnConfig
}

type Session struct {
	cfg     SessionConfig
	cluster *transport.Cluster
}

func NewSession(cfg SessionConfig) (*Session, error) {
	cluster, err := transport.NewCluster(cfg.ConnConfig, cfg.Events, cfg.Hosts...)
	if err != nil {
		return nil, err
	}

	s := &Session{
		cfg:     cfg,
		cluster: cluster,
	}

	return s, nil
}

func (s *Session) leastBusyConn() *transport.Conn {
	for _, node := range s.cluster.Peers() {
		conn := node.LeastBusyConn()
		if conn != nil {
			return conn
		}
	}

	return nil
}

func (s *Session) NewQuery(content string) Query {
	return Query{stmt: transport.Statement{Content: content, Consistency: s.cfg.DefaultConsistency}}
}

var errNoConnection = fmt.Errorf("no working connection")

func (s *Session) Query(req Query) (Result, error) {
	conn := s.leastBusyConn()
	if conn == nil {
		return Result{}, errNoConnection
	}

	res, err := conn.Query(req.stmt, nil)
	return Result(res), err
}

func (s *Session) Prepare(content string) (Query, error) {
	conn := s.leastBusyConn()
	if conn == nil {
		return Query{}, errNoConnection
	}

	p := s.NewQuery(content)
	res, err := conn.Prepare(p.stmt)

	return Query{stmt: res}, err
}

func (s *Session) Execute(req Query) (Result, error) {
	conn := s.leastBusyConn()
	if conn == nil {
		return Result{}, errNoConnection
	}

	res, err := conn.Execute(req.stmt, nil)
	return Result(res), err
}
