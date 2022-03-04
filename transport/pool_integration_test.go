//go:build integration

package transport

import (
	"testing"
	"time"
)

func TestNodeConnPoolIntegration(t *testing.T) {
	p, err := NewConnPool(TestHost+":9042", ConnConfig{})
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Wait for refiller to fill connections to shards")
	time.Sleep(250 * time.Millisecond)

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
	time.Sleep(250 * time.Millisecond)

	t.Log("Check if connections have been refilled")
	for i, c := range p.AllConns() {
		if c == nil {
			t.Fatalf("no conn for shard %d", i)
		}
	}

	t.Log("Close pool")
	p.Close()
	time.Sleep(250 * time.Millisecond)

	t.Log("Check if connections are closed")
	for _, c := range p.AllConns() {
		if c != nil {
			t.Fatalf("conn %s", c)
		}
	}
}

//func TestCloseConnPoolIntegration(t *testing.T) {
//	cp := NewConnPool(TestHost+":19042", ConnConfig{})
//	if cp == nil {
//		t.Fatal("Couldn't start first connection.")
//	}
//
//	time.Sleep(2 * time.Second)
//
//	go func() {
//		for idx := range cp.conns {
//			if conn := cp.loadConn(uint16(idx)); conn != nil {
//				conn.Close()
//			}
//		}
//	}()
//
//	cp.Close()
//
//	time.Sleep(10 * time.Millisecond)
//
//	for idx := range cp.conns {
//		if conn := cp.loadConn(uint16(idx)); conn != nil {
//			t.Fatal("Not all connections have been closed.")
//		}
//	}
//}
//
//func TestConnPoolConnIntegration(t *testing.T) {
//	cp := NewConnPool(TestHost+":19042", ConnConfig{})
//	if cp == nil {
//		t.Fatal("Couldn't start first connection.")
//	}
//
//	if cp.NextConn() == nil {
//		t.Fatal("NextConn returns nil even though initConnPool() has been successful.")
//	}
//
//	time.Sleep(2 * time.Second)
//
//	murmurToken := MurmurToken([]byte(""))
//
//	if conn := cp.Conn(murmurToken); conn == nil || conn.Shard() != 0 {
//		t.Fatal("Invalid return of Conn.")
//	}
//
//	murmurToken = MurmurToken([]byte("0")) // Very big number approx. 3 * 10^18.
//
//	if conn := cp.Conn(murmurToken); conn == nil {
//		t.Fatal("Invalid return of Conn.")
//	}
//
//	go func() {
//		seenNil := false
//		ticker := time.NewTicker(1 * time.Second)
//		for {
//			select {
//			case _ = <-ticker.C:
//				return
//			default:
//				conn := cp.RandConn()
//				if conn == nil {
//					seenNil = true
//				} else if seenNil == true {
//					t.Fatal("Inconsistent RandConn return.")
//				}
//			}
//		}
//	}()
//
//	cp.Close()
//
//	// Waiting for go func().
//	time.Sleep(1 * time.Second)
//}
