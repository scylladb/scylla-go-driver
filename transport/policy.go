package transport

import (
	"go.uber.org/atomic"
	"log"
)

// HostSelectionPolicy prepares plan (slice of Nodes) and returns iterator that goes over it.
// After going through the whole plan, iterator returns nil instead of valid *Node.
type HostSelectionPolicy interface {
	Iter(QueryInfo, int) *Node
	CurrentOffset() int
}

type RoundRobinPolicy struct {
	counter atomic.Int64
	localDC string
}

func NewRoundRobinPolicy(dc string) *RoundRobinPolicy {
	return &RoundRobinPolicy{
		counter: *atomic.NewInt64(0),
		localDC: dc,
	}
}

func (p *RoundRobinPolicy) CurrentOffset() int {
	return int(p.counter.Inc()) - 1
}

func (p *RoundRobinPolicy) Iter(qi QueryInfo, idx int) *Node {
	if p.localDC == "" {
		return qi.topology.nodes[(idx+qi.offset)%len(qi.topology.nodes)]
	}

	var (
		found, target, l, r int
		wantLocal           bool
	)
	for _, n := range qi.topology.nodes {
		if n.datacenter == p.localDC {
			l++
		} else {
			r++
		}
	}

	if idx <= l {
		target = (idx + qi.offset) % l
		wantLocal = true
	} else {
		target = (idx - l + qi.offset) % r
		wantLocal = false
	}
	for _, n := range qi.topology.nodes {
		if (n.datacenter == p.localDC) == wantLocal {
			if target == found {
				return n
			}
			found++
		}
	}

	return nil
}

type TokenAwarePolicy struct {
	RoundRobinPolicy
}

func NewTokenAwarePolicy(dc string) *TokenAwarePolicy {
	return &TokenAwarePolicy{RoundRobinPolicy: RoundRobinPolicy{localDC: dc}}
}

func (p *TokenAwarePolicy) Iter(qi QueryInfo, idx int) *Node {
	// Fallback to policy implemented in wrapperPolicy.
	if !qi.tokenAwareness {
		return p.RoundRobinPolicy.Iter(qi, idx)
	}

	switch qi.strategy.class {
	case simpleStrategy, localStrategy:
		return p.SimpleStrategy(qi, idx)
	case networkTopologyStrategy:
		return p.NetworkTopologyStrategy(qi, idx)
	default:
		log.Printf("host selection policy: query with 'other' strategy class")
		// TODO: add support for other strategies. For now fallback to wrapper.
		return p.RoundRobinPolicy.Iter(qi, idx)
	}
}

func (p *TokenAwarePolicy) SimpleStrategy(qi QueryInfo, idx int) *Node {
	primary := qi.topology.primaryReplica(qi.token)
	target := (idx + qi.offset) % int(qi.strategy.rf)
	if p.localDC == "" {
		return qi.topology.ring[(primary+target)%len(qi.topology.ring)].node
	}

	var (
		found, l, r int
		wantLocal   bool
	)
	for i := 0; i < int(qi.strategy.rf); i++ {
		n := qi.topology.ring[(primary+i)%len(qi.topology.ring)].node
		if n.datacenter == p.localDC {
			l++
		} else {
			r++
		}
	}

	if idx <= l {
		target = (idx + qi.offset) % l
		wantLocal = true
	} else {
		target = (idx - l + qi.offset) % r
		wantLocal = false
	}
	for i := 0; i < int(qi.strategy.rf); i++ {
		n := qi.topology.ring[(primary+i)%len(qi.topology.ring)].node
		if (n.datacenter == p.localDC) == wantLocal {
			if target == found {
				return n
			}
			found++
		}
	}

	return nil
}

func (p *TokenAwarePolicy) NetworkTopologyStrategy(qi QueryInfo, idx int) *Node {
	//resLen := 0
	//// repeats store the amount of nodes from the same rack that we can take in given DC.
	//repeats := make(map[string]int, len(qi.strategy.dcRF))
	//for k, v := range qi.strategy.dcRF {
	//	resLen += int(v)
	//	repeats[k] = int(v) - qi.topology.dcRacks[k]
	//}
	//
	//filter := func(n *Node, res []*Node) bool {
	//	rf := qi.strategy.dcRF[n.datacenter]
	//	fromDC := 0
	//	fromRack := 0
	//	for _, v := range res {
	//		if n.addr == v.addr {
	//			return false
	//		}
	//		if n.datacenter == v.datacenter {
	//			fromDC++
	//			if n.rack == v.rack {
	//				fromRack++
	//			}
	//		}
	//	}
	//
	//	if fromDC < int(rf) {
	//		if fromRack == 0 {
	//			return true
	//		}
	//		if repeats[n.datacenter] > 0 {
	//			repeats[n.datacenter]--
	//			return true
	//		}
	//	}
	//	return false
	//}
	//return qi.topology.replicas(qi.token, resLen, filter)
	return nil
}
