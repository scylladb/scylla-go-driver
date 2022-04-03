package transport

import (
	"github.com/mmatczuk/scylla-go-driver/frame"

	"go.uber.org/atomic"
)

type nodeStatus = atomic.Bool

const (
	statusDown = false
	statusUP   = true
)

type Node struct {
	addr       string
	datacenter string
	rack       string
	tokens     frame.CqlValue // TODO: change it to []Token (implement parsing for Tokens).
	pool       *ConnPool
	status     nodeStatus
}

func (n *Node) Status() bool {
	return n.status.Load()
}

func (n *Node) setStatus(v bool) {
	n.status.Store(v)
}

func (n *Node) LeastBusyConn() *Conn {
	return n.pool.LeastBusyConn()
}
