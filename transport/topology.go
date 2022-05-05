package transport

type topology struct {
	peers       peerMap
	dcRacks     dcRacksMap
	nodes       []*Node
	ring        ring
	ringResized ring
	prepared    preparedNodes
	isPrepared  bool
	keyspaces   ksMap
}

func newTopology() *topology {
	return &topology{
		peers:      make(peerMap),
		dcRacks:    make(dcRacksMap),
		nodes:      make([]*Node, 0),
		ring:       make(ring, 0),
		isPrepared: false,
	}
}

func (t *topology) prepareNodes(defaultKeyspace string) {
	ks, ok := t.keyspaces[defaultKeyspace]
	if !ok {
		return
	}

	switch ks.strategy.class {
	case simpleStrategy, localStrategy:
		t.prepareSimpleStrategy(ks.strategy.rf)
	case networkTopologyStrategy:
		// TODO: special for ML <3. Also is localStrategy the same as simple^?
	}
}

func (t *topology) prepareSimpleStrategy(rf uint32) {
	t.makeResizedRing(rf)
	t.prepared = make(preparedNodes, len(t.ring))
	for i, entry := range t.ring {
		t.prepared[entry.token] = t.ringResized[i : uint32(i)+rf+1]
	}
	t.isPrepared = true
}

// makeResizedRing appends some extra nodes at the end of the slice, so that the cycle will be consistent.
func (t *topology) makeResizedRing(extraSize uint32) {
	l := len(t.ring)
	t.ringResized = make(ring, uint32(l)+extraSize+1)
	for i := range t.ringResized {
		t.ringResized[i] = t.ring[i%l]
	}
}

func (t *topology) preparedReplicas(token Token) []RingEntry {
	return t.prepared[token]
}

// primaryReplicaIdx returns ring index of primary replica that stores data described by token.
func (t *topology) primaryReplicaIdx(token Token) int {
	start, end := 0, len(t.ring)-1
	for start < end {
		mid := int(uint(start+end) >> 1)
		if t.ring[mid].token < token {
			start = mid + 1
		} else {
			end = mid
		}
	}
	return start
}
