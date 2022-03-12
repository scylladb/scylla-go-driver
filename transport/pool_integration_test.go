//go:build integration

package transport

import (
	"math"
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

func TestConnPoolConn(t *testing.T) {
	p, err := NewConnPool(TestHost+":9042", ConnConfig{})
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

	murmurToken := MurmurToken([]byte(""))
	if conn := p.Conn(murmurToken); conn == nil || conn.Shard() != 0 {
		t.Fatal("invalid return of Conn")
	}

	load := uint32(math.Floor(maxStreamID*heavyLoadThreshold + 1))
	p.Conn(murmurToken).metrics.InQueue.Store(load)
	conn := p.Conn(murmurToken)
	if conn == nil {
		t.Fatal("invalid return of Conn")
	} else if conn.Shard() == 0 {
		t.Fatalf("invalid load distribution")
	}

	murmurToken = MurmurToken([]byte("0")) // Very big number approx. 3 * 10^18.
	if conn := p.Conn(murmurToken); conn == nil {
		t.Fatal("invalid return of Conn")
	}

	p.Close()
}
