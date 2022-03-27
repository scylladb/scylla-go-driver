package transport

import (
	"go.uber.org/atomic"
)

type QueryInfo struct {
	token    *Token
	topology *topology

	// TODO: change those two so that necessary data is retrieved from keyspace.
	dcRF map[string]int
	rf   int
}

// HostSelectionPolicy prepares plan (slice of Nodes) and returns iterator that goes over it.
// After going through the whole plan, iterator returns nil instead of valid Node.
type HostSelectionPolicy interface {
	PlanIter(QueryInfo) func() *Node
}

// WrapperPolicy is used to combine round-robin policies with token aware ones.
type WrapperPolicy interface {
	HostSelectionPolicy
	WrapPlan([]*Node) func() *Node
}

// In both round-robin policies counter has to be taken modulo length of node slice.

type roundRobinPolicy struct {
	counter atomic.Int64
}

func newRoundRobinPolicy() roundRobinPolicy {
	return roundRobinPolicy{counter: *atomic.NewInt64(0)}
}

func (r *roundRobinPolicy) PlanIter(qi QueryInfo) func() *Node {
	return r.WrapPlan(qi.topology.allNodes)
}

func (r *roundRobinPolicy) WrapPlan(plan []*Node) func() *Node {
	i := int(r.counter.Inc() - 1)
	start := i

	return func() *Node {
		if i == start+len(plan) {
			return nil
		}

		defer func() { i++ }()
		return plan[i%len(plan)]
	}
}

type dcAwareRoundRobinPolicy struct {
	counter atomic.Int64
	localDC string
}

func newDCAwareRoundRobin(dc string) dcAwareRoundRobinPolicy {
	return dcAwareRoundRobinPolicy{
		counter: *atomic.NewInt64(0),
		localDC: dc,
	}
}

func (d *dcAwareRoundRobinPolicy) PlanIter(qi QueryInfo) func() *Node {
	return d.WrapPlan(qi.topology.allNodes)
}

func (d *dcAwareRoundRobinPolicy) WrapPlan(plan []*Node) func() *Node {
	i := d.counter.Inc() - 1
	local := make([]*Node, 0)
	remote := make([]*Node, 0)
	for _, n := range plan {
		if d.localDC == n.datacenter {
			local = append(local, n)
		} else {
			remote = append(remote, n)
		}
	}

	lit := (&roundRobinPolicy{counter: *atomic.NewInt64(i)}).WrapPlan(local)
	rit := (&roundRobinPolicy{counter: *atomic.NewInt64(i)}).WrapPlan(remote)
	return func() *Node {
		if n := lit(); n != nil {
			return n
		} else {
			return rit()
		}
	}
}

type tokenAwarePolicy struct {
	// TODO: information about strategy can also be retrieved from keyspace.
	simpleStrategy bool
	wrapperPolicy  WrapperPolicy
}

func newTokenAwarePolicy(simple bool, pw WrapperPolicy) tokenAwarePolicy {
	return tokenAwarePolicy{
		simpleStrategy: simple,
		wrapperPolicy:  pw,
	}
}

func (t *tokenAwarePolicy) PlanIter(qi QueryInfo) func() *Node {
	// Fallback to policy implemented in wrapperPolicy.
	if qi.token == nil {
		return t.wrapperPolicy.PlanIter(qi)
	}

	if t.simpleStrategy {
		return t.wrapperPolicy.WrapPlan(t.simpleStrategyReplicas(qi))
	} else {
		return t.wrapperPolicy.WrapPlan(t.networkTopologyStrategyReplicas(qi))
	}
}

func (t *tokenAwarePolicy) simpleStrategyReplicas(qi QueryInfo) []*Node {
	return qi.topology.ringRange(*qi.token, qi.rf, func(n *Node, replicas []*Node) bool {
		for _, v := range replicas {
			if n.addr == v.addr {
				return false
			}
		}
		return true
	})
}

func (t *tokenAwarePolicy) networkTopologyStrategyReplicas(qi QueryInfo) []*Node {
	resLen := 0
	// repeats store the amount of nodes from the same rack that we can take in given DC.
	repeats := make(map[string]int, len(qi.dcRF))
	for k, v := range qi.dcRF {
		resLen += v
		repeats[k] = v - qi.topology.racksInDC[k]
	}

	wanted := func(n *Node, replicas []*Node) bool {
		rf := qi.dcRF[n.datacenter]
		fromDC := 0
		fromRack := 0
		for _, v := range replicas {
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

		if fromDC < rf {
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
	return qi.topology.ringRange(*qi.token, resLen, wanted)
}
