package scylla

import (
	"fmt"
	"time"

	"github.com/mmatczuk/scylla-go-driver/frame"
	"github.com/mmatczuk/scylla-go-driver/transport"
)

// TODO: Add retry policy.
// TODO: Add Query Paging.
// TODO: Merge with host selection policy.

type EventType = string

const (
	TopologyChange EventType = "TOPOLOGY_CHANGE"
	StatusChange   EventType = "STATUS_CHANGE"
	SchemaChange   EventType = "SCHEMA_CHANGE"
)

type Consistency = uint16

const (
	ANY         Consistency = 0x0000
	ONE         Consistency = 0x0001
	TWO         Consistency = 0x0002
	THREE       Consistency = 0x0003
	QUORUM      Consistency = 0x0004
	ALL         Consistency = 0x0005
	LOCALQUORUM Consistency = 0x0006
	EACHQUORUM  Consistency = 0x0007
	SERIAL      Consistency = 0x0008
	LOCALSERIAL Consistency = 0x0009
	LOCALONE    Consistency = 0x000A
)

var (
	ErrNoHosts   = fmt.Errorf("error in session config: no hosts given")
	ErrEventType = fmt.Errorf("error in session config: invalid event\npossible events:\n" +
		"TopologyChange EventType = \"TOPOLOGY_CHANGE\"\n" +
		"StatusChange   EventType = \"STATUS_CHANGE\"\n" +
		"SchemaChange   EventType = \"SCHEMA_CHANGE\"")
	ErrConsistency = fmt.Errorf("error in session config: invalid consistency\npossible consistencies are:\n" +
		"ANY         Consistency = 0x0000\n" +
		"ONE         Consistency = 0x0001\n" +
		"TWO         Consistency = 0x0002\n" +
		"THREE       Consistency = 0x0003\n" +
		"QUORUM      Consistency = 0x0004\n" +
		"ALL         Consistency = 0x0005\n" +
		"LOCALQUORUM Consistency = 0x0006\n" +
		"EACHQUORUM  Consistency = 0x0007\n" +
		"SERIAL      Consistency = 0x0008\n" +
		"LOCALSERIAL Consistency = 0x0009\n" +
		"LOCALONE    Consistency = 0x000A")
	errNoConnection = fmt.Errorf("no working connection")
)

type SessionConfig struct {
	Hosts              []string
	Keyspace           string
	Events             []EventType
	TCPNoDelay         bool
	ConnectionTimeout  time.Duration
	DefaultConsistency Consistency
}

func (s *SessionConfig) copy() *SessionConfig {
	var cfgCopy SessionConfig

	cfgCopy.Hosts = make([]string, len(s.Hosts))
	copy(cfgCopy.Hosts, s.Hosts)

	cfgCopy.Keyspace = s.Keyspace

	cfgCopy.Events = make([]string, len(s.Events))
	copy(cfgCopy.Events, s.Events)

	cfgCopy.TCPNoDelay = s.TCPNoDelay
	cfgCopy.ConnectionTimeout = s.ConnectionTimeout
	cfgCopy.DefaultConsistency = s.DefaultConsistency

	return &cfgCopy
}

func (s *SessionConfig) validate() error {
	if len(s.Hosts) == 0 {
		return ErrNoHosts
	}

	for _, e := range s.Events {
		if e != TopologyChange && e != StatusChange && e != SchemaChange {
			return ErrEventType
		}
	}

	if s.DefaultConsistency > LOCALONE {
		return ErrConsistency
	}

	// Use default timeout if not set.
	if s.ConnectionTimeout == 0 {
		s.ConnectionTimeout = time.Second
	}

	return nil
}

type Session struct {
	cfg     *SessionConfig
	cluster *transport.Cluster
}

func NewSession(config *SessionConfig) (*Session, error) {
	cfg := config.copy()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	connCfg := transport.ConnConfig{
		TCPNoDelay:         cfg.TCPNoDelay,
		Timeout:            cfg.ConnectionTimeout,
		DefaultConsistency: cfg.DefaultConsistency,
	}

	cluster, err := transport.NewCluster(connCfg, cfg.Events, cfg.Hosts...)
	if err != nil {
		return nil, err
	}

	s := &Session{
		cfg:     cfg,
		cluster: cluster,
	}

	return s, nil
}

// FIXME: to be replaced by host selection policy.
func (s *Session) leastBusyConn() *transport.Conn {
	for _, node := range s.cluster.Peers() {
		conn := node.LeastBusyConn()
		if conn != nil {
			return conn
		}
	}

	return nil
}

func (s *Session) Query(content string) Query {
	return Query{session: s,
		stmt: transport.Statement{Content: content, Consistency: s.cfg.DefaultConsistency},
		exec: func(conn *transport.Conn, stmt transport.Statement, pagingState frame.Bytes) (transport.QueryResult, error) {
			return conn.Query(stmt, pagingState)
		},
	}
}

func (s *Session) Prepare(content string) (Query, error) {
	conn := s.leastBusyConn()
	if conn == nil {
		return Query{}, errNoConnection
	}

	stmt := transport.Statement{Content: content, Consistency: s.cfg.DefaultConsistency}
	res, err := conn.Prepare(stmt)

	return Query{session: s,
		stmt: res,
		exec: func(conn *transport.Conn, stmt transport.Statement, pagingState frame.Bytes) (transport.QueryResult, error) {
			return conn.Execute(stmt, pagingState)
		},
	}, err
}
