//go:build integration

package transport

import (
	"scylla-go-driver/frame"
	"testing"
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
	conn, err := OpenShardConn("172.19.0.2:19042", si, ConnConfig{}, TestStreamIDAllocator{})
	if err != nil {
		t.Fatal(err)
	}
	opts := frame.StartupOptions{
		"CQL_VERSION": "3.0.0",
	}
	resp, err := conn.Startup(opts)
	t.Logf("%T, %+v", resp, resp)
	if err != nil {
		t.Fatal(err)
	}
}
