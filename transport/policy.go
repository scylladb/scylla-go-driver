package transport

import (
	"go.uber.org/atomic"
)

// HostSelectionPolicy prepares plan (slice of Nodes) and returns iterator that goes over it.
// After going through the whole plan, iterator returns nil instead of valid *Node.
type HostSelectionPolicy interface {
	New(t *Topology, stg Strategy) HostSelectionPolicy
	Iter(qi QueryInfo, idx int) *Node
	NewQueryInfo() QueryInfo
	NewTokenAwareQueryInfo(t Token) QueryInfo
}

type WrapperPolicy interface {
	HostSelectionPolicy
	WrapIter(reps []*Node, idx, off int) *Node
}

// QueryInfo represents data required for host selection policy to rout query.
// During Query execution QueryInfo does not change (in contrary to queried index).
type QueryInfo struct {
	tokenAwareness bool
	token          Token
	offset         int // Used by Round Robin type policies.
}

type RoundRobinPolicy struct {
	Counter *atomic.Int64
	Nodes   []*Node
}

func (p *RoundRobinPolicy) New(t *Topology, _ Strategy) HostSelectionPolicy {
	return &RoundRobinPolicy{
		Counter: atomic.NewInt64(0),
		Nodes:   t.nodes,
	}
}

func NewRoundRobinPolicy() *RoundRobinPolicy {
	return &RoundRobinPolicy{}
}

func (p *RoundRobinPolicy) Iter(qi QueryInfo, idx int) *Node {
	return p.WrapIter(p.Nodes, idx, qi.offset)
}

func (p *RoundRobinPolicy) WrapIter(reps []*Node, idx, off int) *Node {
	if len(reps) <= idx {
		return nil
	}
	return reps[(idx+off)%len(reps)]
}

func (p *RoundRobinPolicy) NewQueryInfo() QueryInfo {
	return QueryInfo{
		tokenAwareness: false,
		offset:         p.generateOffset(),
	}
}

func (p *RoundRobinPolicy) NewTokenAwareQueryInfo(t Token) QueryInfo {
	return QueryInfo{
		tokenAwareness: true,
		token:          t,
		offset:         p.generateOffset(),
	}
}

func (p *RoundRobinPolicy) generateOffset() int {
	return int(p.Counter.Inc()) - 1
}

type DCAwareRoundRobinPolicy struct {
	RoundRobinPolicy
	LocalDC string
}

func (p *DCAwareRoundRobinPolicy) New(t *Topology, _ Strategy) HostSelectionPolicy {
	return &DCAwareRoundRobinPolicy{
		RoundRobinPolicy: RoundRobinPolicy{
			Counter: atomic.NewInt64(0),
			Nodes:   t.nodes,
		},
		LocalDC: p.LocalDC,
	}
}

func NewDCAwareRoundRobinPolicy(dc string) *DCAwareRoundRobinPolicy {
	return &DCAwareRoundRobinPolicy{
		LocalDC: dc,
	}
}

func (p *DCAwareRoundRobinPolicy) Iter(qi QueryInfo, idx int) *Node {
	return p.WrapIter(p.Nodes, idx, qi.offset)
}

func (p *DCAwareRoundRobinPolicy) WrapIter(reps []*Node, idx, off int) *Node {
	if len(reps) <= idx {
		return nil
	}
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
	if idx < l {
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

type SimpleTokenAwarePolicy struct {
	WrapperPolicy
	Ring         Ring
	ExtendedRing []*Node
	RF           int
}

func (p *SimpleTokenAwarePolicy) New(t *Topology, stg Strategy) HostSelectionPolicy {
	if stg.rf == 0 {
		return &SimpleTokenAwarePolicy{
			WrapperPolicy: p.WrapperPolicy.New(t, stg).(WrapperPolicy),
		}
	}
	extendedRing := make([]*Node, len(t.ring)+stg.rf)
	for i, v := range t.ring {
		extendedRing[i] = v.node
	}
	for i := 0; i < stg.rf; i++ {
		extendedRing[len(t.ring)+i] = t.ring[i].node
	}
	return &SimpleTokenAwarePolicy{
		WrapperPolicy: p.WrapperPolicy.New(t, stg).(WrapperPolicy),
		Ring:          t.ring,
		ExtendedRing:  extendedRing,
		RF:            stg.rf,
	}
}

func NewSimpleTokenAwarePolicy(wp WrapperPolicy) *SimpleTokenAwarePolicy {
	return &SimpleTokenAwarePolicy{
		WrapperPolicy: wp,
	}
}

func (p *SimpleTokenAwarePolicy) Iter(qi QueryInfo, idx int) *Node {
	if qi.tokenAwareness && p.RF != 0 {
		start, end := 0, len(p.Ring)
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

type NetworkTopologyTokenAwarePolicy struct {
	WrapperPolicy
	PreparedNodes []TokenReplicas
}

func (p *NetworkTopologyTokenAwarePolicy) New(t *Topology, stg Strategy) HostSelectionPolicy {
	if stg.dcRF == nil {
		return &NetworkTopologyTokenAwarePolicy{
			WrapperPolicy: p.WrapperPolicy.New(t, stg).(WrapperPolicy),
		}
	}
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
	repeats := make(map[string]int, len(stg.dcRF))
	repsLen := 0
	for _, v := range stg.dcRF {
		repsLen += int(v)
	}

	holder := make([]*Node, repsLen*len(t.ring))
	preparedNodes := make([]TokenReplicas, len(t.ring))
	for i := range preparedNodes {
		preparedNodes[i].Nodes = holder[i*repsLen : (i+1)*repsLen]
		for k, v := range stg.dcRF {
			repeats[k] = int(v) - dcRacks[k]
		}
		taken := 0
		for j := 0; ; j++ {
			n := t.ring[(i+j)%len(t.ring)].node
			rf := int(stg.dcRF[n.datacenter])
			fromDC := 0
			fromRack := 0
			for k := 0; k < taken; k++ {
				v := preparedNodes[i].Nodes[k]
				if n.datacenter == v.datacenter {
					fromDC++
					if n.rack == v.rack {
						fromRack++
					}
				}
			}
			if fromDC < rf {
				if fromRack == 0 {
					preparedNodes[i].Nodes[taken] = n
					taken++
				} else if repeats[n.datacenter] > 0 {
					repeats[n.datacenter]--
					preparedNodes[i].Nodes[taken] = n
					taken++
				}
			}
			if taken == repsLen {
				break
			}
		}
	}
	return &NetworkTopologyTokenAwarePolicy{
		WrapperPolicy: p.WrapperPolicy.New(t, stg).(WrapperPolicy),
		PreparedNodes: preparedNodes,
	}
}

func NewNetworkTopologyTokenAwarePolicy(wp WrapperPolicy) *NetworkTopologyTokenAwarePolicy {
	return &NetworkTopologyTokenAwarePolicy{
		WrapperPolicy: wp,
	}
}

func (p NetworkTopologyTokenAwarePolicy) Iter(qi QueryInfo, idx int) *Node {
	if qi.tokenAwareness && p.PreparedNodes != nil {
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
	} else {
		return p.WrapperPolicy.Iter(qi, idx)
	}
}
