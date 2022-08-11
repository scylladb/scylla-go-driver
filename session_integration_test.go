//go:build integration

package scylla

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/netip"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/scylladb/scylla-go-driver/frame"
	"github.com/scylladb/scylla-go-driver/frame/response"
	"github.com/scylladb/scylla-go-driver/transport"
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

func TestSchemaAgreementIntegration(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)
	defer cancel()

	session := newTestSession(ctx, t)
	defer session.Close()

	stmts := []string{
		"DROP KEYSPACE mykeyspace",
		"CREATE KEYSPACE mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"ALTER KEYSPACE mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2}",
		"CREATE TABLE mykeyspace.users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))",
		"INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (1, 'rick', 'sanchez')",
	}

	for rep := 0; rep < 5; rep++ {
		for _, stmt := range stmts {
			q := session.Query(stmt)
			if _, err := q.Exec(ctx); err != nil {
				t.Fatal(err)
			}

			if agreement, err := session.CheckSchemaAgreement(ctx); err != nil {
				t.Fatal(err)
			} else if !agreement {
				t.Fatal("schema is not in agreement after finishing a query")
			}
		}
	}

	cfg := testingSessionConfig
	cfg.AutoAwaitSchemaAgreementTimeout = 0

	unsafeSession, err := NewSession(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer unsafeSession.Close()

	for rep := 0; rep < 5; rep++ {
		for _, stmt := range stmts {
			q := unsafeSession.Query(stmt)
			if _, err := q.Exec(ctx); err != nil {
				t.Fatal(err)
			}

			if agreement, err := unsafeSession.CheckSchemaAgreement(ctx); err != nil {
				t.Fatal(err)
			} else if !agreement {
				t.Log("schema is not in agreement after finishing a query, awaiting agreement")
				if err := unsafeSession.AwaitSchemaAgreement(ctx, 60*time.Second); err != nil {
					t.Fatal(err)
				}
			}

			if agreement, err := unsafeSession.CheckSchemaAgreement(ctx); err != nil {
				t.Fatal(err)
			} else if !agreement {
				t.Log("schema is not in agreement after finishing a query, awaiting agreement")
				if err := unsafeSession.AwaitSchemaAgreement(ctx, 60*time.Second); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

type execFunc = func(context.Context, *transport.Conn, transport.Statement, frame.Bytes) (transport.QueryResult, error)

type execWrapper struct {
	execCnt         int
	queryRecipients []netip.Addr
	fakeErrors      []error
}

func getIP(addr net.Addr) netip.Addr {
	return netip.MustParseAddrPort(addr.String()).Addr()
}

func (w *execWrapper) wrapExec(exec execFunc, t *testing.T) execFunc {
	return func(ctx context.Context, conn *transport.Conn, stmt transport.Statement, b frame.Bytes) (transport.QueryResult, error) {
		w.queryRecipients = append(w.queryRecipients, getIP(conn.RemoteAddr()))
		w.execCnt++
		t.Log("retrying", conn)
		if w.execCnt <= len(w.fakeErrors) {
			return transport.QueryResult{}, w.fakeErrors[w.execCnt-1]
		}

		return exec(ctx, conn, stmt, b)
	}
}

// reset should always be called before a testcase.
func (w *execWrapper) reset() {
	w.execCnt = 0
	w.queryRecipients = nil
}

func TestExecRetryIntegration(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)
	defer cancel()

	session := newTestSession(ctx, t)
	defer session.Close()

	initStmts := []string{
		"DROP KEYSPACE IF EXISTS retryks",
		"CREATE KEYSPACE IF NOT EXISTS retryks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
		"CREATE TABLE IF NOT EXISTS retryks.t (pk bigint PRIMARY KEY)",
	}

	for _, stmt := range initStmts {
		q := session.Query(stmt)
		if _, err := q.Exec(ctx); err != nil {
			t.Fatal(err)
		}

		// Await schema agreement, TODO: implement true schema agreement.
		time.Sleep(time.Second)
	}
	session.Close()

	cfg := testingSessionConfig
	cfg.Keyspace = "retryks"
	session, err := NewSession(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	for i := 0; i < len(retryTestCases); i++ {
		tc := retryTestCases[i]
		pk := int64(i)
		t.Run(tc.name, func(t *testing.T) {
			tc.execWrapper.reset()
			q, err := session.Prepare(ctx, "INSERT INTO retryks.t (pk) VALUES (?)")
			if err != nil {
				t.Fatal(err)
			}
			q.exec = tc.execWrapper.wrapExec(q.exec, t)
			q.SetIdempotent(tc.idempotent)

			// Insert a single row, check for success
			_, err = q.BindInt64(0, pk).Exec(ctx)
			if err != nil && !tc.shouldFail {
				t.Fatalf("query resulted in error: %v, when it should succeed", err)
			}
			if err == nil && tc.shouldFail {
				t.Fatalf("expected query failure, but got success")
			}

			if len(tc.decisions)+1 != len(tc.execWrapper.queryRecipients) {
				t.Fatalf("expected %d executions, performed %d", len(tc.decisions)-1, len(tc.execWrapper.queryRecipients))
			}

			recipients := tc.execWrapper.queryRecipients
			for i, decision := range tc.decisions {
				if decision == transport.RetryNextNode && recipients[i] == recipients[i+1] {
					t.Fatalf("retry no. %d, expected other node retry, got %v twice", i+1, recipients[i])
				} else if decision == transport.RetrySameNode && recipients[i] != recipients[i+1] {
					t.Fatalf("retry no. %d, expected same node retry, but retry happened to %v after %v", i+1, recipients[i+1], recipients[i])
				}
			}

			// Sanity check, on a failed query row shouldn't be present.
			q, err = session.Prepare(ctx, "SELECT * FROM retryks.t WHERE pk=?")
			if err != nil {
				t.Fatal(err)
			}

			if res, err := q.BindInt64(0, pk).Exec(ctx); err != nil {
				t.Fatal(err)
			} else if len(res.Rows) > 0 && tc.shouldFail {
				t.Fatalf("query returned %d rows, when they shouldn't be inserted in the first place", len(res.Rows))
			} else if len(res.Rows) == 0 && !tc.shouldFail {
				t.Fatal("query returned no rows, when there should be one", len(res.Rows))
			}
		})
	}
}

func TestIterRetryIntegration(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)
	defer cancel()

	session := newTestSession(ctx, t)
	initStmts := []string{
		"DROP KEYSPACE IF EXISTS retryks",
		"CREATE KEYSPACE IF NOT EXISTS retryks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
		"CREATE TABLE IF NOT EXISTS retryks.t (pk bigint PRIMARY KEY)",
	}

	for _, stmt := range initStmts {
		q := session.Query(stmt)
		if _, err := q.Exec(ctx); err != nil {
			t.Fatal(err)
		}

		// Await schema agreement, TODO: implement true schema agreement.
		time.Sleep(time.Second)
	}
	session.Close()

	cfg := testingSessionConfig
	cfg.Keyspace = "retryks"
	session, err := NewSession(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	for i := 0; i < len(retryTestCases); i++ {
		tc := retryTestCases[i]
		pk := int64(i)
		t.Run(tc.name, func(t *testing.T) {
			tc.execWrapper.reset()
			insertQ, err := session.Prepare(ctx, "INSERT INTO retryks.t (pk) VALUES (?)")
			if err != nil {
				t.Fatal(err)
			}
			selectQ, err := session.Prepare(ctx, "SELECT * FROM retryks.t WHERE pk=?")
			if err != nil {
				t.Fatal(err)
			}
			selectQ.exec = tc.execWrapper.wrapExec(selectQ.exec, t)
			selectQ.SetIdempotent(tc.idempotent)

			_, err = insertQ.BindInt64(0, pk).Exec(ctx)
			if err != nil {
				t.Fatal(err)
			}

			// Select a single row, check for success
			it := selectQ.BindInt64(0, pk).Iter(ctx)
			_, err = it.Next()
			if err != nil && !tc.shouldFail {
				t.Fatalf("query resulted in error: %v, when it should succeed", err)
			}
			if err == nil && tc.shouldFail {
				t.Fatalf("expected query failure, but got success")
			}

			if len(tc.decisions)+1 != len(tc.execWrapper.queryRecipients) {
				t.Fatalf("expected %d executions, performed %d", len(tc.decisions)-1, len(tc.execWrapper.queryRecipients))
			}

			recipients := tc.execWrapper.queryRecipients
			for i, decision := range tc.decisions {
				if decision == transport.RetryNextNode && recipients[i] == recipients[i+1] {
					t.Fatalf("retry no. %d, expected other node retry, got %v twice", i+1, recipients[i])
				} else if decision == transport.RetrySameNode && recipients[i] != recipients[i+1] {
					t.Fatalf("retry no. %d, expected same node retry, but retry happened to %v after %v", i+1, recipients[i+1], recipients[i])
				}
			}
		})
	}
}

var retryTestCases = []struct {
	name        string
	execWrapper execWrapper
	idempotent  bool
	shouldFail  bool
	decisions   []transport.RetryDecision
}{
	{
		name:        "success without retries",
		execWrapper: execWrapper{},
		shouldFail:  false,
	},
	{
		name: "ErrCodeSyntax",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeSyntax}},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeSyntax",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeSyntax}},
		},
		shouldFail: true,
		idempotent: true,
	},
	{
		name: "ErrCodeInvalid",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeInvalid}},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeInvalid",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeInvalid}},
		},
		shouldFail: true,
		idempotent: true,
	},
	{
		name: "ErrCodeAlreadyExists",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: frame.ErrCodeAlreadyExists}},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeAlreadyExists",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeAlreadyExists}},
		},
		shouldFail: true,
		idempotent: true,
	},
	{
		name: "ErrCodeFunctionFailure",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeFunctionFailure}},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeFuncFailure",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeFunctionFailure}},
		},
		shouldFail: true,
		idempotent: true,
	},
	{
		name: "ErrCodeCredentials",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeCredentials}},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeCredentials",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeCredentials}},
		},
		shouldFail: true,
		idempotent: true,
	},
	{
		name: "ErrCodeUnauthorized",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeUnauthorized}},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeUnauthorized",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeUnauthorized}},
		},
		shouldFail: true,
		idempotent: true,
	},
	{
		name: "ErrCodeConfig",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeConfig}},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeConfig",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeConfig}},
		},
		shouldFail: true,
		idempotent: true,
	},
	{
		name: "ErrCodeReadFailure",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ReadFailureError{
					ScyllaError: response.ScyllaError{Code: ErrCodeReadFailure},
					Consistency: frame.TWO,
					Received:    2,
					BlockFor:    1,
					NumFailures: 1,
					DataPresent: false,
				}},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeReadFailure",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ReadFailureError{
					ScyllaError: response.ScyllaError{Code: ErrCodeReadFailure},
					Consistency: frame.TWO,
					Received:    2,
					BlockFor:    1,
					NumFailures: 1,
					DataPresent: false,
				}},
		},
		shouldFail: true,
	},
	{
		name: "ErrCodeWriteFailure",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ReadFailureError{
					ScyllaError: response.ScyllaError{Code: ErrCodeWriteFailure},
					Consistency: frame.TWO,
					Received:    2,
					BlockFor:    1,
					NumFailures: 1,
					DataPresent: false,
				}},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeWriteFailure",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.WriteFailureError{
					ScyllaError: response.ScyllaError{Code: ErrCodeWriteFailure},
					Consistency: frame.TWO,
					Received:    2,
					BlockFor:    1,
					NumFailures: 1,
					WriteType:   frame.BatchLog,
				}},
		},
		shouldFail: true,
	},
	{
		name: "ErrCodeUnprepared",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeUnprepared}},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeUnprepared",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeUnprepared}},
		},
		shouldFail: true,
		idempotent: true,
	},
	{
		name: "ErrCodeOverloaded",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeOverloaded}},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeOverloaded",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ScyllaError{Code: ErrCodeOverloaded},
				response.ScyllaError{Code: ErrCodeOverloaded},
			},
		},
		decisions:  []transport.RetryDecision{transport.RetryNextNode, transport.RetryNextNode},
		idempotent: true,
	},
	{
		name: "ErrCodeTruncate",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeTruncate}},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeTruncate",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ScyllaError{Code: ErrCodeTruncate},
				response.ScyllaError{Code: ErrCodeTruncate},
			},
		},
		decisions:  []transport.RetryDecision{transport.RetryNextNode, transport.RetryNextNode},
		idempotent: true,
	},
	{
		name: "ErrCodeServer",
		execWrapper: execWrapper{
			fakeErrors: []error{response.ScyllaError{Code: ErrCodeServer}},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeServer",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ScyllaError{Code: ErrCodeServer},
				response.ScyllaError{Code: ErrCodeServer},
			},
		},
		decisions:  []transport.RetryDecision{transport.RetryNextNode, transport.RetryNextNode},
		idempotent: true,
	},
	{
		name: "IO error",
		execWrapper: execWrapper{
			fakeErrors: []error{fmt.Errorf("dummy error")},
		},
		shouldFail: true,
	},
	{
		name: "idempotent IO error",
		execWrapper: execWrapper{
			fakeErrors: []error{fmt.Errorf("dummy error")},
		},
		shouldFail: true,
	},
	{
		name: "ErrCodeBootstrapping",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ScyllaError{Code: ErrCodeBootstrapping},
				response.ScyllaError{Code: ErrCodeBootstrapping},
			},
		},
		decisions:  []transport.RetryDecision{transport.RetryNextNode, transport.RetryNextNode},
		idempotent: true,
	},
	{
		name: "idempotent ErrCodeBootstrapping",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ScyllaError{Code: ErrCodeBootstrapping},
				response.ScyllaError{Code: ErrCodeBootstrapping},
			},
		},
		decisions:  []transport.RetryDecision{transport.RetryNextNode, transport.RetryNextNode},
		idempotent: true,
	},
	{
		name: "ErrCodeUnavailable",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.UnavailableError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeUnavailable},
					Consistency: frame.TWO,
					Required:    2,
					Alive:       1,
				},
				response.UnavailableError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeUnavailable},
					Consistency: frame.TWO,
					Required:    2,
					Alive:       1,
				},
			},
		},
		decisions:  []transport.RetryDecision{transport.RetryNextNode},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeUnavailable",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.UnavailableError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeUnavailable},
					Consistency: frame.TWO,
					Required:    2,
					Alive:       1,
				},
				response.UnavailableError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeUnavailable},
					Consistency: frame.TWO,
					Required:    2,
					Alive:       1,
				},
			},
		},
		decisions:  []transport.RetryDecision{transport.RetryNextNode},
		shouldFail: true,
		idempotent: true,
	},
	{
		name: "ErrCodeReadTimeout, enough responses, data present",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ReadTimeoutError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeReadTimeout},
					Consistency: frame.TWO,
					Received:    2,
					BlockFor:    2,
					DataPresent: true,
				},
				response.ReadTimeoutError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeReadTimeout},
					Consistency: frame.TWO,
					Received:    2,
					BlockFor:    2,
					DataPresent: true,
				},
			},
		},
		decisions:  []transport.RetryDecision{transport.RetrySameNode},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeReadTimeout, enough responses, data present",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ReadTimeoutError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeReadTimeout},
					Consistency: frame.TWO,
					Received:    2,
					BlockFor:    2,
					DataPresent: true,
				},
				response.ReadTimeoutError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeReadTimeout},
					Consistency: frame.TWO,
					Received:    2,
					BlockFor:    2,
					DataPresent: true,
				},
			},
		},
		decisions:  []transport.RetryDecision{transport.RetrySameNode},
		shouldFail: true,
		idempotent: true,
	},
	{
		name: "ErrCodeReadTimeout, enough responses, data not present",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ReadTimeoutError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeReadTimeout},
					Consistency: frame.TWO,
					Received:    2,
					BlockFor:    2,
					DataPresent: false,
				},
			},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeReadTimeout, enough responses, data not present",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ReadTimeoutError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeReadTimeout},
					Consistency: frame.TWO,
					Received:    2,
					BlockFor:    2,
					DataPresent: false,
				},
			},
		},
		shouldFail: true,
		idempotent: true,
	},
	{
		name: "ErrCodeReadTimeout, not enough responses, data present",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ReadTimeoutError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeReadTimeout},
					Consistency: frame.TWO,
					Received:    1,
					BlockFor:    2,
					DataPresent: true,
				},
			},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeReadTimeout, enough responses, data present",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.ReadTimeoutError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeReadTimeout},
					Consistency: frame.TWO,
					Received:    1,
					BlockFor:    2,
					DataPresent: true,
				},
			},
		},
		shouldFail: true,
		idempotent: true,
	},
	{
		name: "ErrCodeWriteTimeout, type == batchLog",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.WriteTimeoutError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeWriteTimeout},
					Consistency: frame.TWO,
					Received:    1,
					BlockFor:    2,
					WriteType:   frame.BatchLog,
				},
			},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeWriteTimeout, type == batchLog",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.WriteTimeoutError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeWriteTimeout},
					Consistency: frame.TWO,
					Received:    1,
					BlockFor:    2,
					WriteType:   frame.BatchLog,
				},
				response.WriteTimeoutError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeWriteTimeout},
					Consistency: frame.TWO,
					Received:    1,
					BlockFor:    2,
					WriteType:   frame.BatchLog,
				},
			},
		},
		decisions:  []transport.RetryDecision{transport.RetrySameNode},
		shouldFail: true,
		idempotent: true,
	},
	{
		name: "ErrCodeWriteTimeout, type != batchLog",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.WriteTimeoutError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeWriteTimeout},
					Consistency: frame.TWO,
					Received:    4,
					BlockFor:    2,
					WriteType:   frame.Simple,
				},
			},
		},
		shouldFail: true,
	},
	{
		name: "idempotent ErrCodeWriteTimeout, type != batchLog",
		execWrapper: execWrapper{
			fakeErrors: []error{
				response.WriteTimeoutError{
					ScyllaError: response.ScyllaError{Code: frame.ErrCodeWriteTimeout},
					Consistency: frame.TWO,
					Received:    4,
					BlockFor:    2,
					WriteType:   frame.Simple,
				},
			},
		},
		shouldFail: true,
		idempotent: true,
	},
}
