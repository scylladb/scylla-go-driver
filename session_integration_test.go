//go:build integration

package scylla

import (
	"testing"
)

const TestHost = "192.168.100.100"

func newTestSession() (*Session, error) {
	config := SessionConfig{
		Hosts:              []string{TestHost + ":9042"},
		TCPNoDelay:         false,
		DefaultConsistency: 1,
	}

	return NewSession(&config)
}

func TestSessionIntegration(t *testing.T) { // nolint:paralleltest // Integration test are not run in parallel!
	session, err := newTestSession()

	if err != nil {
		t.Fatal("couldn't start session")
	}

	stmts := []string{
		"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE IF NOT EXISTS mykeyspace.users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))",
		"INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (1, 'rick', 'sanchez')",
		"INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (4, 'rust', 'cohle')",
	}

	for _, stmt := range stmts {
		q := session.Query(stmt)
		if _, err = q.Exec(); err != nil {
			t.Fatal(err)
		}
	}

	q := session.Query("SELECT * FROM mykeyspace.users")

	res, err := q.Exec()
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
	session, err := newTestSession()
	if err != nil {
		t.Fatal("couldn't start session")
	}

	initStmts := []string{
		"DROP KEYSPACE IF EXISTS mykeyspace",
		"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE IF NOT EXISTS mykeyspace.triples (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)",
	}

	for _, stmt := range initStmts {
		q := session.Query(stmt)
		if _, err = q.Exec(); err != nil {
			t.Fatal(err)
		}
	}

	insertQuery, err := session.Prepare(insertStmt)
	if err != nil {
		t.Fatal(err)
	}

	selectQuery, err := session.Prepare(selectStmt)
	if err != nil {
		t.Fatal(err)
	}

	for i := int64(0); i < 100; i++ {
		insertQuery.BindInt64(0, i).BindInt64(1, 2*i).BindInt64(2, 3*i)
		res, err := insertQuery.Exec()
		if err != nil {
			t.Fatal(err)
		}

		selectQuery.BindInt64(0, i)
		res, err = selectQuery.Exec()
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
