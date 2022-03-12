package transport

import (
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
	tokens     []byte // TODO: change it to []Token (implement parsing for Tokens).
	pool       *ConnPool
	status     nodeStatus
}
