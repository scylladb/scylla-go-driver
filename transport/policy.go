package transport

import (
	"go.uber.org/atomic"
)

// HostSelectionPolicy prepares plan (slice of Nodes) and returns iterator that goes over it.
// After going through the whole plan, iterator returns nil instead of valid *Node.
type HostSelectionPolicy interface {
	Iter(qi QueryInfo, idx int) *Node
	Update(t *topology)
	GenerateOffset() int
	Clone() HostSelectionPolicy
}

type WrapperPolicy interface {
	HostSelectionPolicy
	WrapIter(reps []*Node, idx, off int) *Node
	CloneWrapper() WrapperPolicy
}

type RoundRobinPolicy struct {
	Counter *atomic.Int64
	Nodes   []*Node
}

func (p *RoundRobinPolicy) Iter(qi QueryInfo, idx int) *Node {
	return p.WrapIter(p.Nodes, idx, qi.offset)
}

func (p *RoundRobinPolicy) WrapIter(reps []*Node, idx, off int) *Node {
	return reps[(idx+off)%len(reps)]
}

func (p *RoundRobinPolicy) Update(t *topology) {
	p.Nodes = t.nodes
}

func (p *RoundRobinPolicy) GenerateOffset() int {
	return int(p.Counter.Inc()) - 1
}

func (p *RoundRobinPolicy) CloneWrapper() WrapperPolicy {
	nodesClone := make([]*Node, len(p.Nodes))
	copy(nodesClone, p.Nodes)

	return &RoundRobinPolicy{
		Counter: atomic.NewInt64(0),
		Nodes:   nodesClone,
	}
}

func (p *RoundRobinPolicy) Clone() HostSelectionPolicy {
	nodesClone := make([]*Node, len(p.Nodes))
	copy(nodesClone, p.Nodes)

	return &RoundRobinPolicy{
		Counter: atomic.NewInt64(0),
		Nodes:   nodesClone,
	}
}

func NewRoundRobinPolicy() *RoundRobinPolicy {
	return &RoundRobinPolicy{Counter: atomic.NewInt64(0)}
}

type DCAwareRoundRobinPolicy struct {
	RoundRobinPolicy
	LocalDC string
}

func (p *DCAwareRoundRobinPolicy) Iter(qi QueryInfo, idx int) *Node {
	return p.WrapIter(p.Nodes, idx, qi.offset)
}

func (p *DCAwareRoundRobinPolicy) WrapIter(reps []*Node, idx, off int) *Node {
	if p.LocalDC == "" {
		return p.RoundRobinPolicy.WrapIter(reps, idx, off)
	}
	var (
		found, target, l, r int
		wantLocal           bool
	)
	for _, n := range reps {
		if n.datacenter == p.LocalDC {
			l++
		} else {
			r++
		}
	}
	if idx <= l {
		target = (idx + off) % l
		wantLocal = true
	} else {
		target = (idx - l + off) % r
		wantLocal = false
	}
	for _, n := range reps {
		if (n.datacenter == p.LocalDC) == wantLocal {
			if target == found {
				return n
			}
			found++
		}
	}
	return nil
}

func (p *DCAwareRoundRobinPolicy) Clone() HostSelectionPolicy {
	return p // TODO: implement valid Clone method.
}

func NewDCAwareRoundRobin(dc string) *DCAwareRoundRobinPolicy {
	return &DCAwareRoundRobinPolicy{
		RoundRobinPolicy: RoundRobinPolicy{Counter: atomic.NewInt64(0)},
		LocalDC:          dc,
	}
}

type SimpleTokenAwarePolicy struct {
	WrapperPolicy
	Ring         Ring
	ExtendedRing []*Node
	RF           int
}

func (p *SimpleTokenAwarePolicy) Iter(qi QueryInfo, idx int) *Node {
	if qi.tokenAwareness {
		start, end := 0, len(p.Ring)-p.RF
		for start < end {
			mid := int(uint(start+end) >> 1)
			if p.Ring[mid].token < qi.token {
				start = mid + 1
			} else {
				end = mid
			}
		}
		return p.WrapIter(p.ExtendedRing[start:start+p.RF], idx, qi.offset)
	} else {
		return p.WrapperPolicy.Iter(qi, idx)
	}
}

func (p *SimpleTokenAwarePolicy) Update(t *topology) {
	p.Ring = t.ring
	p.ExtendedRing = make([]*Node, len(p.Ring)+p.RF)
	for i, v := range p.Ring {
		p.ExtendedRing[i] = v.node
	}
	for i := 0; i < p.RF; i++ {
		p.ExtendedRing[len(p.Ring)+i] = p.Ring[i].node
	}
	p.WrapperPolicy.Update(t)
}

func (p *SimpleTokenAwarePolicy) Clone() HostSelectionPolicy {
	ringClone := make(Ring, len(p.Ring))
	wp := p.WrapperPolicy.CloneWrapper()

	return &SimpleTokenAwarePolicy{
		WrapperPolicy: wp,
		Ring:          ringClone,
		RF:            p.RF,
	}
}

func NewSimpleTokenAwarePolicy(wp WrapperPolicy, rf int) *SimpleTokenAwarePolicy {
	return &SimpleTokenAwarePolicy{
		WrapperPolicy: wp,
		RF:            rf,
	}
}

type NetworkTopologyTokenAwarePolicy struct {
	WrapperPolicy
	PreparedNodes []TokenReplicas
	DCrf          map[string]int
}

func NewNetworkTopologyTokenAwarePolicy(wp WrapperPolicy, dcRf map[string]int) *NetworkTopologyTokenAwarePolicy {
	return &NetworkTopologyTokenAwarePolicy{
		WrapperPolicy: wp,
		DCrf:          dcRf,
	}
}

func (p NetworkTopologyTokenAwarePolicy) Iter(qi QueryInfo, idx int) *Node {
	start, end := 0, len(p.PreparedNodes)
	for start < end {
		mid := int(uint(start+end) >> 1)
		if p.PreparedNodes[mid].Token < qi.token {
			start = mid + 1
		} else {
			end = mid
		}
	}
	if start == len(p.PreparedNodes) {
		start = 0
	}
	return p.WrapIter(p.PreparedNodes[start].Nodes, idx, qi.offset)
}

func (p *NetworkTopologyTokenAwarePolicy) Update(t *topology) {
	type uniqueRack struct {
		dc   string
		rack string
	}
	u := make(map[uniqueRack]struct{})
	dcRacks := make(map[string]int)
	for _, n := range t.nodes {
		u[uniqueRack{dc: n.datacenter, rack: n.rack}] = struct{}{}
	}
	for k := range u {
		dcRacks[k.dc]++
	}

	repeats := make(map[string]int, len(p.DCrf))
	repsLen := 0
	for k, v := range p.DCrf {
		repsLen += v
		repeats[k] = v - dcRacks[k]
	}

	holder := make([]*Node, repsLen*len(t.ring))
	p.PreparedNodes = make([]TokenReplicas, len(t.ring))
	for i := range p.PreparedNodes {
		p.PreparedNodes[i].Nodes = holder[(i-1)*repsLen : i*repsLen]
		taken := 0
		for j := 0; ; j++ {
			n := t.ring[(i+j)%len(t.ring)].node
			rf := p.DCrf[n.datacenter]
			fromDC := 0
			fromRack := 0
			for k := 0; k < taken; k++ {
				v := p.PreparedNodes[i].Nodes[k]
				if n.datacenter == v.datacenter {
					fromDC++
					if n.rack == v.rack {
						fromRack++
					}
				}
			}

			if fromDC < rf {
				if fromRack == 0 {
					p.PreparedNodes[i].Nodes[taken] = n
					taken++
				} else if repeats[n.datacenter] > 0 {
					repeats[n.datacenter]--
					p.PreparedNodes[i].Nodes[taken] = n
					taken++
				}
			}
			if taken == repsLen {
				break
			}
		}
	}
}

func (p *NetworkTopologyTokenAwarePolicy) Clone() HostSelectionPolicy {
	return p // TODO: implement valid Clone method.
}
