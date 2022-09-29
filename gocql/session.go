package gocql

import (
	"context"
	"errors"
	"time"

	"github.com/scylladb/scylla-go-driver"
)

type Session struct {
	session         *scylla.Session
	cfg             scylla.SessionConfig
	control         SingleHostQueryExecutor
	schemaDescriber *schemaDescriber
}

func NewSession(cfg ClusterConfig) (*Session, error) {
	scfg, err := sessionConfigFromGocql(&cfg)
	if err != nil {
		return nil, err
	}

	session, err := scylla.NewSession(context.Background(), scfg)
	if err != nil {
		return nil, err
	}
	s := &Session{
		session: session,
		cfg:     scfg,
	}

	s.control, err = NewSingleHostQueryExecutor(&cfg)
	if err != nil {
		return nil, err
	}

	s.cfg.RetryPolicy = transformRetryPolicy(cfg.RetryPolicy)
	s.schemaDescriber = newSchemaDescriber(s)
	return s, nil
}

func (s *Session) Query(stmt string, values ...interface{}) *Query {
	return &Query{
		ctx:    context.Background(),
		query:  s.session.Query(stmt),
		values: values,
	}
}

func (s *Session) Close() {
	s.session.Close()
	s.control.Close()
}

func (s *Session) Closed() bool {
	return s.session.Closed()
}

func (s *Session) AwaitSchemaAgreement(ctx context.Context) error {
	s.session.AwaitSchemaAgreement(context.Background(), time.Minute)
	return nil
}

var (
	ErrSessionClosed = errors.New("session closed")
	ErrNoKeyspace    = errors.New("no keyspace")
)

// KeyspaceMetadata returns the schema metadata for the keyspace specified. Returns an error if the keyspace does not exist.
func (s *Session) KeyspaceMetadata(keyspace string) (*KeyspaceMetadata, error) {
	// fail fast
	if s.Closed() {
		return nil, ErrSessionClosed
	} else if keyspace == "" {
		return nil, ErrNoKeyspace
	}

	return s.schemaDescriber.getSchema(keyspace)
}
