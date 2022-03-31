//go:build integration

package scylla

import (
	"strconv"
	"sync"
	"testing"
)

var benchmarkResult Result
var inserts = 10000

func createBenchTable(session *Session, b *testing.B) {
	b.Helper()
	stmts := []string{
		"DROP KEYSPACE sessionbenchks",
		"CREATE KEYSPACE sessionbenchks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE sessionbenchks.users (id bigint, v bigint, PRIMARY KEY((id)))",
	}
	for _, stmt := range stmts {
		q := session.NewQuery(stmt)
		_, err := session.Query(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryIntegration(b *testing.B) {
	var (
		r       Result
		err     error
		session *Session
	)

	session, err = runDriver()
	if err != nil {
		b.Fatal(err)
	}
	createBenchTable(session, b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < inserts; i++ {
			id := n*inserts + inserts
			insert := session.NewQuery("INSERT INTO sessionbenchks.users(id, v) VALUES (" + strconv.Itoa(id) + "," + strconv.Itoa(id))
			_, err = session.Query(insert)
			if err != nil {
				b.Fatal(err)
			}

			query := session.NewQuery("SELECT v FROM sessionbenchks.users WHERE id = " + strconv.Itoa(id))
			r, err = session.Query(query)
			if err != nil {
				b.Fatal(err)
			}

			if v, err := r.Rows[0][0].AsInt64(); v != int64(n) || err != nil {
				b.Fatalf("invalid value")
			}
		}
	}

	benchmarkResult = r
}

func BenchmarkAsyncQueryIntegration(b *testing.B) {
	var (
		err     error
		session *Session
		wg      sync.WaitGroup

		insertCallback = func(fn Result, err error) {
			b.Helper()
			if err != nil {
				b.Fatal(err)
			}

			wg.Done()
		}
	)

	session, err = runDriver()
	if err != nil {
		b.Fatal(err)
	}
	createBenchTable(session, b)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		wg.Add(inserts)
		for i := 0; i < inserts; i++ {
			id := n*inserts + inserts
			insert := session.NewQuery("INSERT INTO sessionbenchks.users(id, v) VALUES (" + strconv.Itoa(id) + "," + strconv.Itoa(id))
			session.QueryAsync(insert, insertCallback)
		}
		wg.Wait()

		wg.Add(inserts)
		for i := 0; i < inserts; i++ {
			id := n*inserts + inserts
			query := session.NewQuery("SELECT v FROM sessionbenchks.users WHERE id = " + strconv.Itoa(id))
			session.QueryAsync(query, func(fn Result, err error) {
				b.Helper()
				if err != nil {
					b.Fatal(err)
				}

				if v, _ := fn.Rows[0][0].AsInt64(); v != int64(id) {
					b.Fatalf("invalid value")

				}
				wg.Done()
			})
		}
		wg.Wait()
	}
}
