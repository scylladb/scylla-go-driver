//go:build integration

package transport

import (
	"math"
	"testing"
	"time"
)

const refillerBackoff = 500 * time.Millisecond

func TestConnPoolIntegration(t *testing.T) {
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
	p, err := NewConnPool(TestHost, ConnConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	t.Log("Wait for refiller to fill connections to shards")
	time.Sleep(refillerBackoff)

	t.Log("Check if connections to shards are established")
	for i, c := range p.AllConns() {
		if c == nil {
			t.Fatalf("no conn for shard %d", i)
		}
	}

	t0 := MurmurToken([]byte(""))
	if conn := p.Conn(t0); conn == nil || conn.Shard() != 0 {
		t.Fatal("invalid return of Conn")
	}

	load := uint32(math.Floor(maxStreamID*heavyLoadThreshold + 1))
	p.Conn(t0).metrics.InQueue.Store(load)

	if conn := p.Conn(t0); conn == nil {
		t.Fatal("invalid return of Conn")
	} else if conn.Shard() == 0 {
		t.Fatalf("invalid load distribution")
	}

	t1 := MurmurToken([]byte("0")) // Very big number approx. 3 * 10^18.
	if conn := p.Conn(t1); conn == nil {
		t.Fatal("invalid return of Conn")
	}
}

func TestValidAddressParsing(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		address  string
		port     string
		expected string
	}{
		{
			name:     "simple swap",
			address:  "192.168.100.1:8258",
			port:     "9862",
			expected: "192.168.100.1:9862",
		},
		{
			name:     "no port set",
			address:  "192.168.100.1",
			port:     "9862",
			expected: "192.168.100.1:9862",
		},
		{
			name:     "ipv6 with port",
			address:  "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]:8258",
			port:     "9862",
			expected: "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]:9862",
		},
		{
			name:     "ipv6 no port",
			address:  "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]",
			port:     "9862",
			expected: "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]:9862",
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if res := getShardAwareAddress(tc.address, tc.port); res != tc.expected {
				t.Fatal("Failure while extracting address")
			}
		})
	}
}

func TestInvalidAddressParsing(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		address string
		port    string
	}{
		{
			name:    "too many colons IPv4",
			address: "192.168.100.1:1:1",
			port:    "9862",
		},
		{
			name:    "no square brackets IPv6",
			address: "2a02:a311:433f:9580:e16:b5d2:6f06:c897",
			port:    "9862",
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			defer func() {
				if err := recover(); err == nil {
					t.Fatal("Parsed invalid IP addresses")
				}
			}()
			_ = getShardAwareAddress(tc.address, tc.port)
		})
	}
}
