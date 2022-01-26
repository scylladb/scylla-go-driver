//go:build integration

package transport

import (
	"net"
	"testing"

	"scylla-go-driver/frame"
)

func TestConnStartup(t *testing.T) {
	nc, err := net.Dial("tcp", "localhost:9042")
	if err != nil {
		t.Fatal(err)
	}
	conn := WrapConn(nc)

	opts := frame.StartupOptions{
		"CQL_VERSION": "3.0.0",
	}
	resp, err := conn.Startup(opts)
	t.Logf("%T, %+v", resp, resp)
	if err != nil {
		t.Fatal(err)
	}
}
