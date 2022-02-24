package scylla

import (
	"fmt"
	"net"
	"time"

	"scylla-go-driver/transport"
)

// TODO: Merge with working cluster.
// TODO: Should we support connecting to different than default port?
// TODO: Add retry policy.
// TODO: Add Query Paging.

type (
	Result = transport.QueryResult
	Query  = transport.Statement
)

type SessionConfig struct {
	hosts []net.IP
	transport.ConnConfig
}

// MakeSessionConfig takes hosts addresses without port! Driver assumes default port is 9042.
func MakeSessionConfig(tcpNoDelay bool, defaultConsistency uint16, hosts ...string) SessionConfig {
	return SessionConfig{
		hosts: discoverHosts(hosts),
		ConnConfig: transport.ConnConfig{
			TCPNoDelay:         tcpNoDelay,
			Timeout:            5 * time.Second, // Should user have control of timeout value?
			DefaultConsistency: defaultConsistency,
		},
	}
}

func discoverHosts(hosts []string) []net.IP {
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
	cluster interface{} // transport.Cluster
}

// TODO: Remove this placeholder.
func NewCluster(cfg transport.ConnConfig, a interface{}, hosts interface{}) (interface{}, error) {
	return nil, nil
}

func NewSession(cfg SessionConfig) (*Session, error) {
	cluster, err := NewCluster(cfg.ConnConfig, nil, cfg.hosts) // transport.NewCluster(cfg.ConnConfig, nil, cfg.hosts)
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
	return transport.NewStatement(content)
}

// TODO: Remove this placeholder
func GetRandomConnectionFromCluster() *transport.Conn {
	return nil
}

func (s *Session) Query(req Query) (Result, error) {
	conn := GetRandomConnectionFromCluster()
	if conn == nil {
		return Result{}, fmt.Errorf("connection not available")
	}

	return conn.Query(req, nil)
}
