package transport

import (
	"go.uber.org/atomic"
)

type QueryInfo struct {
	token    Token
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

// PolicyWrapper is used to combine round-robin policies with token aware ones.
type PolicyWrapper interface {
	WrapPlan([]*Node) func() *Node
}

// In both round-robin policies it is important that:
// - counter has to be taken modulo length of node slice
// - initially we set counter to -1 because it is incremented each time we read it

type roundRobinPolicy struct {
	counter atomic.Int64
}

func newRoundRobinPolicy() roundRobinPolicy {
	return roundRobinPolicy{counter: *atomic.NewInt64(-1)}
}

func (r *roundRobinPolicy) PlanIter(qi QueryInfo) func() *Node {
	return r.WrapPlan(qi.topology.allNodes)
}

func (r *roundRobinPolicy) WrapPlan(plan []*Node) func() *Node {
	i := int(r.counter.Inc())
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
		counter: *atomic.NewInt64(-1),
		localDC: dc,
	}
}

func (d *dcAwareRoundRobinPolicy) PlanIter(qi QueryInfo) func() *Node {
	return d.WrapPlan(qi.topology.allNodes)
}

func (d *dcAwareRoundRobinPolicy) WrapPlan(plan []*Node) func() *Node {
	i := int(d.counter.Inc())
	start := i

	local := make([]*Node, 0)
	remote := make([]*Node, 0)
	for _, n := range plan {
		if d.localDC == n.datacenter {
			local = append(local, n)
		} else {
			remote = append(remote, n)
		}
	}

	return func() *Node {
		if i == start+len(plan) {
			return nil
		}

		defer func() { i++ }()
		index := i % len(plan)

		if index < len(local) {
			return local[index]
		} else {
			return remote[index-len(local)]
		}
	}
}

type tokenAwarePolicy struct {
	// TODO: information about strategy can also be retrieved from keyspace.
	simpleStrategy bool
	wrapper        PolicyWrapper
}

func newTokenAwarePolicy(simple bool, pw PolicyWrapper) tokenAwarePolicy {
	if pw == nil {
		return tokenAwarePolicy{
			simpleStrategy: simple,
			wrapper:        defaultWrapper{},
		}
	} else {
		return tokenAwarePolicy{
			simpleStrategy: simple,
			wrapper:        pw,
		}
	}
}

func (p *tokenAwarePolicy) PlanIter(qi QueryInfo) func() *Node {
	if p.simpleStrategy {
		return p.wrapper.WrapPlan(p.simpleStrategyReplicas(qi))
	} else {
		return p.wrapper.WrapPlan(p.networkTopologyStrategyReplicas(qi))
	}
}

func (p *tokenAwarePolicy) simpleStrategyReplicas(qi QueryInfo) []*Node {
	return qi.topology.ringRange(qi.token, qi.rf, func(n *Node, replicas []*Node) bool {
		for _, v := range replicas {
			if n.addr == v.addr {
				return false
			}
		}
		return true
	})
}

func (p *tokenAwarePolicy) networkTopologyStrategyReplicas(qi QueryInfo) []*Node {
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
	return qi.topology.ringRange(qi.token, resLen, wanted)
}

// defaultWrapper is used only when no plan wrapper is defined for token aware policy.
type defaultWrapper struct{}

func (d defaultWrapper) WrapPlan(plan []*Node) func() *Node {
	counter := 0
	return func() *Node {
		if counter == len(plan) {
			return nil
		}

		defer func() { counter++ }()
		return plan[counter]
	}
}
