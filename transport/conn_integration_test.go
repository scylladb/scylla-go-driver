//go:build integration

package transport

import (
	"net"
	"sync"
	"testing"
	"time"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestOpenShardConnIntegration(t *testing.T) {
	si := ShardInfo{
		Shard:    1,
		NrShards: 2, // Note that scylla node from docker-compose has only 2 shards.
	}

	// TODO check shard info from supported
	// Note that only direct IP calls ensures correct shard mapping.
	// I tested it manually using time.sleep() and checking if connection was mapped to appropriate shard with cqlsh ("SELECT * FROM system.clients;").
	_, err := OpenShardConn("127.0.0.1:19042", si, ConnConfig{Timeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
}

func TestConnMassiveQueryIntegration(t *testing.T) {
	nc, err := net.Dial("tcp", "localhost:9042")
	if err != nil {
		t.Fatal(err)
	}

	conn, err := WrapConn(nc)
	if err != nil {
		t.Fatal(err)
	}

	exec := func(cql string) {
		t.Helper()
		s := Statement{
			Content:     cql,
			Consistency: frame.ONE,
		}
		if _, err = conn.Query(s, nil); err != nil {
			t.Fatal(err)
		}
	}

	exec("CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")
	exec("CREATE TABLE IF NOT EXISTS mykeyspace.users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))")
	exec("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (1, 'rick', 'sanchez')")
	exec("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (4, 'rust', 'cohle')")

	query := Statement{Content: "SELECT * FROM mykeyspace.users", Consistency: frame.ONE}
	expected := []frame.Row{
		{
			frame.Bytes{0x0, 0x0, 0x0, 0x1},
			frame.Bytes{'r', 'i', 'c', 'k'},
			frame.Bytes{'s', 'a', 'n', 'c', 'h', 'e', 'z'},
		},
		{
			frame.Bytes{0x0, 0x0, 0x0, 0x4},
			frame.Bytes{'r', 'u', 's', 't'},
			frame.Bytes{'c', 'o', 'h', 'l', 'e'},
		},
	}

	const n = 1500

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			res, err := conn.Query(query, nil)
			if err != nil {
				t.Fatal(err)
			}

			if len(res.Rows) != 2 {
				t.Fatal("invalid number of rows")
			}

			for j, row := range res.Rows {
				if diff := cmp.Diff(expected[j], row); diff != "" {
					t.Fatal(diff)
				}
			}
		}()
	}

	wg.Wait()
}
