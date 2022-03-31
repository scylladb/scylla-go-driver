//go:build integration

package scylla

import (
	"testing"

	"scylla-go-driver/transport"
)

const TestHost = "192.168.100.100"

func runDriver() (*Session, error) {
	config := SessionConfig{Hosts: []string{TestHost + ":9042"},
		Keyspace:   "",
		Events:     nil,
		ConnConfig: transport.ConnConfig{TCPNoDelay: false, Timeout: 1, DefaultConsistency: 1},
	}
	return NewSession(&config)
}

func TestSessionIntegration(t *testing.T) {
	session, err := runDriver()

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
		q := session.NewQuery(stmt)
		session.Query(q)
	}

	q := session.NewQuery("SELECT * FROM mykeyspace.users")

	res, err := session.Query(q)
	if err != nil {
		t.Fatalf("couldn't query: %v", err)
	}

	for _, row := range res.Rows {
		pk, _ := row[0].AsInt32()
		name, _ := row[1].AsText()
		surname, _ := row[2].AsText()

		t.Log(pk, name, surname)
	}
}

const (
	insertStmt = "INSERT INTO mykeyspace.triples (pk, v1, v2) VALUES(?, ?, ?)"
	selectStmt = "SELECT v1, v2 FROM mykeyspace.triples WHERE pk = ?"
)

func TestSessionPrepareIntegration(t *testing.T) {
	session, err := runDriver()
	if err != nil {
		t.Fatal("couldn't start session")
	}

	initStmts := []string{
		"DROP KEYSPACE IF EXISTS mykeyspace",
		"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE IF NOT EXISTS mykeyspace.triples (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)",
	}

	for _, stmt := range initStmts {
		q := session.NewQuery(stmt)
		session.Query(q)
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
		insertQuery.BindInt64(0, i)
		insertQuery.BindInt64(1, 2*i)
		insertQuery.BindInt64(2, 3*i)

		res, err := session.Execute(insertQuery)
		if err != nil {
			t.Fatal(err)
		}

		selectQuery.BindInt64(0, i)
		res, err = session.Execute(selectQuery)
		if err != nil {
			t.Fatal(err)
		}

		if len(res.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(res.Rows))
		}

		v1, _ := res.Rows[0][0].AsInt64()
		v2, _ := res.Rows[0][1].AsInt64()
		if v1 != 2*i || v2 != 3*i {
			t.Fatalf("expected (%d, %d), got (%d, %d)", 2*i, 3*i, v1, v2)
		}
	}
}
