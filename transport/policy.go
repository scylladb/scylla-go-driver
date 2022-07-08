package transport

import (
	"log"

	"go.uber.org/atomic"
)

// HostSelectionPolicy prepares plan (slice of Nodes) and returns iterator that goes over it.
// After going through the whole plan, iterator returns nil instead of valid *Node.
type HostSelectionPolicy interface {
	PlanIter(QueryInfo) func() *Node
}

// WrapperPolicy is used to combine round-robin policies with token aware ones.
type WrapperPolicy interface {
	HostSelectionPolicy
	WrapPlan([]*Node) func() *Node
}

type RoundRobinPolicy struct {
	counter atomic.Int64 // Counter has to be taken modulo length of node slice.
}

func NewRoundRobinPolicy() *RoundRobinPolicy {
	return &RoundRobinPolicy{counter: *atomic.NewInt64(0)}
}

func (p *RoundRobinPolicy) PlanIter(qi QueryInfo) func() *Node {
	return p.WrapPlan(qi.topology.nodes)
}

func (p *RoundRobinPolicy) WrapPlan(plan []*Node) func() *Node {
	i := int(p.counter.Inc() - 1)
	start := i
	return func() *Node {
		if i == start+len(plan) {
			return nil
		}
		defer func() { i++ }()
		return plan[i%len(plan)]
	}
}

type DCAwareRoundRobinPolicy struct {
	counter atomic.Int64 // Counter has to be taken modulo length of node slice.
	localDC string
}

func NewDCAwareRoundRobin(dc string) *DCAwareRoundRobinPolicy {
	return &DCAwareRoundRobinPolicy{
		counter: *atomic.NewInt64(0),
		localDC: dc,
	}
}

func (p *DCAwareRoundRobinPolicy) PlanIter(qi QueryInfo) func() *Node {
	return p.WrapPlan(qi.topology.nodes)
}

func (p *DCAwareRoundRobinPolicy) WrapPlan(plan []*Node) func() *Node {
	i := p.counter.Inc() - 1
	l := make([]*Node, 0)
	r := make([]*Node, 0)
	for _, n := range plan {
		if p.localDC == n.datacenter {
			l = append(l, n)
		} else {
			r = append(r, n)
		}
	}
	lit := (&RoundRobinPolicy{counter: *atomic.NewInt64(i)}).WrapPlan(l)
	rit := (&RoundRobinPolicy{counter: *atomic.NewInt64(i)}).WrapPlan(r)
	return func() *Node {
		if n := lit(); n != nil {
			return n
		} else {
			return rit()
		}
	}
}

type TokenAwarePolicy struct {
	wrapperPolicy WrapperPolicy
}

func NewTokenAwarePolicy(wp WrapperPolicy) *TokenAwarePolicy {
	return &TokenAwarePolicy{wrapperPolicy: wp}
}

func (p *TokenAwarePolicy) PlanIter(qi QueryInfo) func() *Node {
	// Fallback to policy implemented in wrapperPolicy.
	if !qi.tokenAwareness {
		return p.wrapperPolicy.PlanIter(qi)
	}

	switch qi.strategy.class {
	case simpleStrategy, localStrategy:
		return p.wrapperPolicy.WrapPlan(p.SimpleStrategyReplicas(qi))
	case networkTopologyStrategy:
		return p.wrapperPolicy.WrapPlan(p.NetworkTopologyStrategyReplicas(qi))
	default:
		log.Printf("host selection policy: query with 'other' strategy class")
		// TODO: add support for other strategies. For now fallback to wrapper.
		return p.wrapperPolicy.PlanIter(qi)
	}
}

func (p *TokenAwarePolicy) SimpleStrategyReplicas(qi QueryInfo) []*Node {
	rit := qi.topology.replicaIter(qi.token)
	filter := func(n *Node, res []*Node) bool {
		for _, v := range res {
			if n.hostID == v.hostID {
				return false
			}
		}

		return true
	}

	cur := &qi.topology.trie
	for len(cur.Path()) < int(qi.strategy.rf) {
		n := rit.Next()
		if n == nil {
			return cur.Path()
		}

		if filter(n, cur.Path()) {
			cur = cur.Next(n)
		}
	}

	return cur.Path()
}

func (p *TokenAwarePolicy) NetworkTopologyStrategyReplicas(qi QueryInfo) []*Node {
	desiredCnt := 0
	// repeats store the amount of nodes from the same rack that we can take in given DC.
	repeats := make(map[string]int, len(qi.strategy.dcRF))
	for k, v := range qi.strategy.dcRF {
		desiredCnt += int(v)
		repeats[k] = int(v) - qi.topology.dcRacks[k]
	}

	filter := func(n *Node, res []*Node) bool {
		rf := qi.strategy.dcRF[n.datacenter]
		fromDC := 0
		fromRack := 0
		for _, v := range res {
			if n.addr == v.addr {
				return false
			}
			if n.datacenter == v.datacenter {
				fromDC++
				if n.rack == v.rack {
					fromRack++
				}
			}
		}

		if fromDC < int(rf) {
			if fromRack == 0 {
				return true
			}
			if repeats[n.datacenter] > 0 {
				repeats[n.datacenter]--
				return true
			}
		}
		return false
	}

	rit := qi.topology.replicaIter(qi.token)

	cur := &qi.topology.trie
	for len(cur.Path()) < desiredCnt {
		n := rit.Next()
		if n == nil {
			return cur.Path()
		}

		if filter(n, cur.Path()) {
			cur = cur.Next(n)
		}
	}
	return cur.Path()
}
