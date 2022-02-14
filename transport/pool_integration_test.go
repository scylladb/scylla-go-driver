//go:build integration

package transport

import (
	"testing"
	"time"
)

const refillerBackoff = 250 * time.Millisecond

func TestNodeConnPoolIntegration(t *testing.T) {
	p, err := NewConnPool(TestHost, ConnConfig{})
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Wait for refiller to fill connections to shards")
	time.Sleep(refillerBackoff)

	t.Log("Check if connections to shards are established")
	for i, c := range p.AllConns() {
		if c == nil {
			t.Fatalf("no conn for shard %d", i)
		}
	}

	t.Log("Close connections")
	for _, c := range p.AllConns() {
		c.Close()
	}

	t.Log("Wait for refiller to fill connections to shards")
	time.Sleep(refillerBackoff)

	t.Log("Check if connections have been refilled")
	for i, c := range p.AllConns() {
		if c == nil {
			t.Fatalf("no conn for shard %d", i)
		}
	}

	t.Log("Close pool")
	p.Close()
	time.Sleep(refillerBackoff)

	t.Log("Check if connections are closed")
	for _, c := range p.AllConns() {
		if c != nil {
			t.Fatalf("conn %s", c)
		}
	}
}
