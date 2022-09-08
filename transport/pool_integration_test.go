//go:build integration

package transport

import (
	"context"
	"math"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

const refillerBackoff = 500 * time.Millisecond

func newTestConnPool(ctx context.Context, t *testing.T) *ConnPool {
	p, err := NewConnPool(ctx, TestHost, DefaultConnConfig(""))
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

	return p
}

func TestConnPoolIntegration(t *testing.T) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)
	defer cancel()

	p := newTestConnPool(ctx, t)
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

func TestConnPoolConnIntegration(t *testing.T) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)
	defer cancel()

	p := newTestConnPool(ctx, t)
	defer p.Close()

	t0 := MurmurToken([]byte(""))
	if conn, err := p.Conn(t0); err != nil || conn.Shard() != 0 {
		t.Fatal("invalid return of Conn")
	}

	load := uint32(math.Floor(maxStreamID/2 + 1))

	conn, err := p.Conn(t0)
	if err != nil {
		t.Fatal("invalid return of Conn")
	}

	conn.stats.inQueue.Store(load)

	if conn, err := p.Conn(t0); err != nil {
		t.Fatal("invalid return of Conn")
	} else if conn.Shard() == 0 {
		t.Fatalf("invalid load distribution")
	}

	t1 := MurmurToken([]byte("0")) // Very big number approx. 3 * 10^18.
	if _, err := p.Conn(t1); err != nil {
		t.Fatal("invalid return of Conn")
	}
}
