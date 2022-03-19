package scylla

import (
	"fmt"
	"net"
	"time"

	"scylla-go-driver/transport"
)

// TODO: Should we support connecting to different than default port?
// TODO: Add retry policy.
// TODO: Add Query Paging.
// TODO: make better randomConnection.

type SessionConfig struct {
	hosts []string
	transport.ConnConfig
}

func MakeSessionConfig(tcpNoDelay bool, defaultConsistency uint16, hosts ...string) SessionConfig {
	cpy := make([]string, len(hosts))
	copy(cpy, hosts)

	return SessionConfig{
		hosts: cpy,
		ConnConfig: transport.ConnConfig{
			TCPNoDelay:         tcpNoDelay,
			Timeout:            5 * time.Second, // Should user have control of timeout value?
			DefaultConsistency: defaultConsistency,
		},
	}
}

// I don't know if we need this.
func discoverHosts(hosts []string) []net.IP { // nolint:deadcode,unused // This might be used in the future.
	var res []net.IP
	for _, h := range hosts {
		if ip, err := net.LookupIP(h); err != nil {
			res = append(res, ip[0])
		}
	}
	return res
}

type Session struct {
	cfg     SessionConfig
	cluster *transport.Cluster
}

func NewSession(cfg SessionConfig) (*Session, error) {
	cluster, err := transport.NewCluster(cfg.ConnConfig, nil, cfg.hosts...)
	if err != nil {
		return nil, err
	}

	s := &Session{
		cfg:     cfg,
		cluster: cluster,
	}

	return s, nil
}

func (s *Session) NewQuery(content string) Query {
	return Query{stmt: transport.Statement{Content: content, Consistency: s.cfg.DefaultConsistency}}
}

// TODO: This need to be replaced with proper random, but first let's wait for the node selection mechanism and see how it works.
func (s *Session) randomConnection() *transport.Conn {
	for _, node := range s.cluster.Peers() {
		conn := node.RandomConnection()
		if conn != nil {
			return conn
		}
	}

	return nil
}

var errNoConnection = fmt.Errorf("no working connection")

func (s *Session) Query(req Query) (Result, error) {
	conn := s.randomConnection()
	if conn == nil {
		return Result{}, errNoConnection
	}

	res, err := conn.Query(req.stmt, nil)
	return Result(res), err
}

func (s *Session) Prepare(content string) (Query, error) {
	conn := s.randomConnection()
	if conn == nil {
		return Query{}, errNoConnection
	}

	p := s.NewQuery(content)
	res, err := conn.Prepare(p.stmt)

	return Query{stmt: res}, err
}

func (s *Session) Execute(req Query) (Result, error) {
	conn := s.randomConnection()
	if conn == nil {
		return Result{}, errNoConnection
	}

	res, err := conn.Execute(req.stmt, nil)
	return Result(res), err
}
