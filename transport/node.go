package transport

import (
	"go.uber.org/atomic"
)

type nodeStatus = *atomic.Bool

const (
	statusDown = false
	statusUP   = true
)

type Node struct {
	Addr       string
	Datacenter string
	Rack       string
	Tokens     []byte // It should be []Token, but we don't have parsing for that yet.
	Pool       *ConnPool
	Status     nodeStatus
}

func (n *Node) SetStatus(status bool) {
	n.Status.Store(status)
}

func (n *Node) GetStatus() bool {
	return n.Status.Load()
}
