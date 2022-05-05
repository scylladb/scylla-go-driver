package transport

import (
	"log"

	"go.uber.org/atomic"
)

// HostSelectionPolicy returns i-th valid *Node.
type HostSelectionPolicy interface {
	Iter(QueryInfo) *Node
}

// TokenAwarePolicy auto implements Round Robin mechanism to balance load.
type TokenAwarePolicy struct {
	counter atomic.Int64
}

func NewTokenAwarePolicy() *TokenAwarePolicy {
	return &TokenAwarePolicy{counter: *atomic.NewInt64(0)}
}

func (p *TokenAwarePolicy) Iter(qi QueryInfo) *Node {
	// Fallback to policy implemented in wrapperPolicy.
	if !qi.tokenAwareness {
		// FIXME: what here?
		return qi.topology.ring[0].node
	}

	switch qi.strategy.class {
	case simpleStrategy, localStrategy:
		return p.SimpleStrategy(qi)
	case networkTopologyStrategy:
		return p.NetworkTopologyStrategy(qi)
	default:
		log.Printf("host selection policy: query with 'other' strategy class")
		// TODO: add support for other strategies. What to do here?
		// return p.RoundRobinPolicy.Iter(qi, idx)
		return qi.topology.ring[0].node
	}
}

func (p *TokenAwarePolicy) SimpleStrategy(qi QueryInfo) *Node {
	primaryIdx := qi.topology.primaryReplicaIdx(qi.token)
	if qi.topology.isPrepared {
		replicas := qi.topology.preparedReplicas(qi.topology.ring[primaryIdx].token)
		rr := int64(p.counter.Inc()) - 1
		return replicas[rr%int64(qi.strategy.rf)].node
	}

	return qi.topology.ring[primaryIdx].node
}

func (p *TokenAwarePolicy) NetworkTopologyStrategy(qi QueryInfo) *Node {
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
