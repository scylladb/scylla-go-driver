//go:build integration

package scylla

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"os/signal"
	"syscall"
	"testing"

	"go.uber.org/goleak"
)

const TestHost = "192.168.100.100"

var testingSessionConfig = DefaultSessionConfig("mykeyspace", TestHost)

func initKeyspace(ctx context.Context, t testing.TB) {
	t.Helper()

	cfg := testingSessionConfig
	cfg.Keyspace = ""
	s, err := NewSession(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	q := s.Query("CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")
	if _, err = q.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	s.Close()
}

func newTestSession(ctx context.Context, t testing.TB) *Session {
	t.Helper()

	initKeyspace(ctx, t)
	s, err := NewSession(ctx, testingSessionConfig)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSessionIntegration(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)
	defer cancel()

	session := newTestSession(ctx, t)
	defer session.Close()

	stmts := []string{
		"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE IF NOT EXISTS mykeyspace.users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))",
		"INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (1, 'rick', 'sanchez')",
		"INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (4, 'rust', 'cohle')",
	}

	for _, stmt := range stmts {
		q := session.Query(stmt)
		if _, err := q.Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}

	q := session.Query("SELECT * FROM mykeyspace.users")

	res, err := q.Exec(ctx)
	if err != nil {
		t.Fatalf("couldn't query: %v", err)
	}

	for _, row := range res.Rows {
		pk, err := row[0].AsInt32()
		if err != nil {
			t.Fatal(err)
		}
		name, err := row[1].AsText()
		if err != nil {
			t.Fatal(err)
		}
		surname, err := row[2].AsText()
		if err != nil {
			t.Fatal(err)
		}

		t.Log(pk, name, surname)
	}
}

const (
	insertStmt = "INSERT INTO mykeyspace.triples (pk, v1, v2) VALUES(?, ?, ?)"
	selectStmt = "SELECT v1, v2 FROM mykeyspace.triples WHERE pk = ?"
)

func TestSessionPrepareIntegration(t *testing.T) { // nolint:paralleltest // Integration tests are not run in parallel!
	defer goleak.VerifyNone(t)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)
	defer cancel()

	session := newTestSession(ctx, t)
	defer session.Close()

	initStmts := []string{
		"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE IF NOT EXISTS mykeyspace.triples (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)",
		"TRUNCATE mykeyspace.triples",
	}

	for _, stmt := range initStmts {
		q := session.Query(stmt)
		if _, err := q.Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}

	insertQuery, err := session.Prepare(ctx, insertStmt)
	if err != nil {
		t.Fatal(err)
	}

	selectQuery, err := session.Prepare(ctx, selectStmt)
	if err != nil {
		t.Fatal(err)
	}

	for i := int64(0); i < 100; i++ {
		insertQuery.BindInt64(0, i).BindInt64(1, 2*i).BindInt64(2, 3*i)
		res, err := insertQuery.Exec(ctx)
		if err != nil {
			t.Fatal(err)
		}

		selectQuery.BindInt64(0, i)
		res, err = selectQuery.Exec(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if len(res.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(res.Rows))
		}

		v1, err := res.Rows[0][0].AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		v2, err := res.Rows[0][1].AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		if v1 != 2*i || v2 != 3*i {
			t.Fatalf("expected (%d, %d), got (%d, %d)", 2*i, 3*i, v1, v2)
		}
	}
}

func TestSessionIterIntegration(t *testing.T) { // nolint:paralleltest // Integration tests are not run in parallel!
	defer goleak.VerifyNone(t)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)
	defer cancel()

	session := newTestSession(ctx, t)
	defer session.Close()

	initStmts := []string{
		"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE IF NOT EXISTS mykeyspace.triples (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)",
		"TRUNCATE TABLE mykeyspace.triples",
	}

	for _, stmt := range initStmts {
		q := session.Query(stmt)
		if _, err := q.Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}

	insertQuery, err := session.Prepare(ctx, insertStmt)
	if err != nil {
		t.Fatal(err)
	}

	N := 1000
	for i := int64(0); i < int64(N); i++ {
		insertQuery.BindInt64(0, i).BindInt64(1, 2*i).BindInt64(2, 3*i)

		if _, err := insertQuery.Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}

	q := session.Query("SELECT * FROM mykeyspace.triples")
	q.SetPageSize(10)

	p, err := session.Prepare(ctx, "SELECT * FROM mykeyspace.triples")
	if err != nil {
		t.Fatal(err)
	}
	p.SetPageSize(10)

	iters := [2]Iter{q.Iter(ctx), p.Iter(ctx)}
	for _, it := range iters {
		row, err := it.Next()

		m := make(map[int64]struct{})
		for ; err == nil; row, err = it.Next() {
			x, err := row[0].AsInt64()
			if err != nil {
				t.Fatal(err)
			}
			y, err := row[1].AsInt64()
			if err != nil {
				t.Fatal(err)
			}
			z, err := row[2].AsInt64()
			if err != nil {
				t.Fatal(err)
			}

			if y != 2*x || z != 3*x {
				t.Fatalf("expected (%d, %d, %d), got (%d, %d %d)", x, 2*x, 3*x, x, y, z)
			}
			m[x] = struct{}{}
		}

		if !errors.Is(err, ErrNoMoreRows) {
			t.Fatal(err)
		}

		it.Close()
		_, err = it.Next()
		if err == nil {
			t.Fatal("read on closed iter should fail")
		}

		if len(m) != N {
			t.Fatalf("expected %d different rows, got %d", N, len(m))
		}
	}
}

var (
	caPath   = "testdata/tls/cadb.pem"
	certPath = "testdata/tls/db.crt"
	keyPath  = "testdata/tls/db.key"
)

func newCertPoolFromFile(t *testing.T, path string) *x509.CertPool {
	certPool := x509.NewCertPool()
	pem, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !certPool.AppendCertsFromPEM(pem) {
		t.Fatalf("failed parsing of CA certs")
	}

	return certPool
}

func makeCertificatesFromFiles(t *testing.T, certPath, keyPath string) []tls.Certificate {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}

	return []tls.Certificate{cert}
}

func TestTLSIntegration(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)
	defer cancel()

	testCases := []struct {
		name      string
		tlsConfig *tls.Config
	}{
		{
			name:      "no tls",
			tlsConfig: nil,
		},
		{
			name: "tls - no client verification",
			tlsConfig: &tls.Config{
				RootCAs:            x509.NewCertPool(),
				InsecureSkipVerify: true,
				Certificates:       makeCertificatesFromFiles(t, certPath, keyPath),
			},
		},
		{
			name: "tls - with client verification",
			tlsConfig: &tls.Config{
				RootCAs:            newCertPoolFromFile(t, caPath),
				InsecureSkipVerify: false,
				ServerName:         "192.168.100.100",
				Certificates:       makeCertificatesFromFiles(t, certPath, keyPath),
			},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			cfg := testingSessionConfig.Clone()
			cfg.TLSConfig = tc.tlsConfig
			cfg.Keyspace = ""
			cfg.Hosts = []string{"192.168.100.100"}
			if cfg.TLSConfig != nil {
				cfg.DefaultPort = "9142"
			}

			session, err := NewSession(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()

			stmts := []string{
				"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
				"CREATE TABLE IF NOT EXISTS mykeyspace.users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))",
				"INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (1, 'rick', 'sanchez')",
				"INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (4, 'rust', 'cohle')",
			}

			for _, stmt := range stmts {
				q := session.Query(stmt)
				if _, err := q.Exec(ctx); err != nil {
					t.Fatal(err)
				}
			}

			q := session.Query("SELECT COUNT(*) FROM mykeyspace.users")
			if r, err := q.Exec(ctx); err != nil {
				t.Fatal(err)
			} else {
				n, err := r.Rows[0][0].AsInt64()
				t.Log(n)
				if err != nil {
					t.Fatal(err)
				}

				if n != 2 {
					t.Fatalf("expected 2, got %d", n)
				}
			}
		})
	}
}

func TestPrepareIntegration(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)
	defer cancel()

	session := newTestSession(ctx, t)
	defer session.Close()

	initStmts := []string{
		"DROP KEYSPACE IF EXISTS testks",
		"CREATE KEYSPACE IF NOT EXISTS testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE IF NOT EXISTS testks.doubles (pk bigint PRIMARY KEY, v bigint)",
	}

	for _, stmt := range initStmts {
		q := session.Query(stmt)
		if _, err := q.Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}

	q, err := session.Prepare(ctx, "INSERT INTO testks.doubles (pk, v) VALUES (?, ?)")
	if err != nil {
		t.Fatal(err)
	}

	for i := int64(0); i < 1000; i++ {
		_, err := q.BindInt64(0, i).BindInt64(1, 2*i).Exec(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := int64(0); i < 1000; i++ {
		q, err := session.Prepare(ctx, "SELECT v FROM testks.doubles WHERE pk = "+fmt.Sprint(i))
		if err != nil {
			t.Fatal(err)
		}

		for rep := 0; rep < 3; rep++ {
			res, err := q.Exec(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if v, err := res.Rows[0][0].AsInt64(); err != nil {
				t.Fatal(err)
			} else if v != 2*i {
				t.Fatalf("expected %d, got %d", 2*i, v)
			}
		}
	}
}

func TestContextsIntegration(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)
	defer cancel()

	session := newTestSession(ctx, t)
	defer session.Close()

	initStmts := []string{
		"DROP KEYSPACE IF EXISTS contextks",
		"CREATE KEYSPACE IF NOT EXISTS contextks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE IF NOT EXISTS contextks.t (pk bigint PRIMARY KEY, v bigint)",
	}

	// Before stop queries should succeed.
	for _, stmt := range initStmts {
		q := session.Query(stmt)
		if _, err := q.Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}

	insertQ, err := session.Prepare(ctx, "INSERT INTO contextks.t(pk) VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	selectQ, err := session.Prepare(ctx, "SELECT pk FROM contextks.t WHERE pk=?")
	if err != nil {
		t.Fatal(err)
	}

	// Query on undone context should succeed.
	if _, err := insertQ.BindInt64(0, 1).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	if res, err := selectQ.BindInt64(0, 1).Exec(ctx); err != nil {
		t.Fatal(err)
	} else if n, err := res.Rows[0][0].AsInt64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("expected 1, got %d", n)
	}

	doneCtx, doneCancel := context.WithCancel(ctx)
	doneCancel()
	for i := 0; i < 1000; i++ {
		if _, err := insertQ.BindInt64(0, 2).Exec(doneCtx); err == nil {
			t.Fatal("query on done query context should return an error")
		}
	}

	// Query on undone context should still succeed.
	if res, err := selectQ.BindInt64(0, 2).Exec(ctx); err != nil {
		t.Fatal(err)
	} else if len(res.Rows) > 0 {
		t.Fatal("insert with context done before the query should not reach the database")
	}

	cancel()
	if _, err := selectQ.Exec(ctx); err == nil {
		t.Fatal("query on done session context should return an error")
	}
}
