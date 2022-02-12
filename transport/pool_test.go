package transport

import (
	"testing"
	"time"
)

func TestInitNodeConnPool(t *testing.T) {
	t.Parallel()
	// Initiate pool refiller.
	// Ip of 172.18.0.2 is my computer specific, this is dockers fault, this will be fixed in the near future.
	pr := InitNodeConnPool("172.18.0.2:19042", ConnConfig{})
	// Wait for refiller to fill connections to shards.
	time.Sleep(time.Millisecond)
	// Check if connections to shards are established.
	for _, conn := range pr.conns {
		if conn == nil {
			t.Fatal("pool refiller didn't fill all the connections")
		}
	}

	// Close all connections to shards.
	for _, conn := range pr.conns {
		err := conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Check if pool refilled has established new connections.
	time.Sleep(time.Millisecond)
	for _, conn := range pr.conns {
		if conn == nil {
			t.Fatal("pool refiller didn't fill all the connections after closing one")
		}
	}
}
