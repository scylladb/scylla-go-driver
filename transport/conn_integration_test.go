//go:build integration

package transport

import (
	"net"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"scylla-go-driver/frame"
)

func TestConnStartup(t *testing.T) {
	//nc, err := net.Dial("tcp", "localhost:9042")
	//if err != nil {
	//	t.Fatal(err)
	//}
	//conn := WrapConn(nc, TestStreamIDAllocator{})

	si := ShardInfo{
		Shard:    1,
		NrShards: 2, // Note that scylla node from docker-compose has only 2 shards.
	}
	// Similar problem as in OpenLocalPortConn where only some forms of local IP works fine with
	// shard aware policy. I tested it manually using time.sleep() and checking if connection was
	// mapped to appropriate shard with cqlsh ("SELECT * FROM system.clients;").
	// Here only 172.19.0.2 IP ensures correct shard mapping.
	_, err := OpenShardConn("172.19.0.2:19042", si, ConnConfig{})
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

	clear := Statement{
		Content:     "DROP KEYSPACE IF EXISTS mykeyspace",
		Consistency: frame.ONE,
	}
	createKeySpace := Statement{
		Content: "CREATE KEYSPACE mykeyspace " +
			"WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		Consistency: frame.ONE,
	}
	createTable := Statement{
		Content: "CREATE TABLE mykeyspace.users" +
			" (user_id int, fname text, lname text, PRIMARY KEY((user_id)))",
		Consistency: frame.ONE,
	}
	insert1 := Statement{
		Content:     "insert into mykeyspace.users(user_id, fname, lname) values (1, 'rick', 'sanchez')",
		Consistency: frame.ONE,
	}
	insert2 := Statement{
		Content:     "insert into mykeyspace.users(user_id, fname, lname) values (4, 'rust', 'cohle')",
		Consistency: frame.ONE,
	}

	if _, err = conn.Query(clear, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Query(createKeySpace, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Query(createTable, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Query(insert1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Query(insert2, nil); err != nil {
		t.Fatal(err)
	}

	query := Statement{Content: "SELECT * FROM mykeyspace.users", Consistency: frame.ONE}
	expectedRow1 := frame.Row{
		frame.Bytes{0x0, 0x0, 0x0, 0x1},
		frame.Bytes{'r', 'i', 'c', 'k'},
		frame.Bytes{'s', 'a', 'n', 'c', 'h', 'e', 'z'},
	}
	expectedRow2 := frame.Row{
		frame.Bytes{0x0, 0x0, 0x0, 0x4},
		frame.Bytes{'r', 'u', 's', 't'},
		frame.Bytes{'c', 'o', 'h', 'l', 'e'},
	}
	expected := []frame.Row{expectedRow1, expectedRow2}

	n := 1000

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
