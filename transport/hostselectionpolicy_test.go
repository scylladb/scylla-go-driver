package transport

import (
	"testing"

	"github.com/google/btree"
)

// Round-Robin tests can't be run in parallel because
// we have to know current number of iterations for testing result.

// mockTopologyRoundRobin creates cluster topology with info about 5 nodes living in 2 different datacenters.
func mockTopologyRoundRobin() *topology {
	dummyNodes := []*Node{
		{addr: "1", datacenter: "eu"},
		{addr: "2", datacenter: "eu"},
		{addr: "3", datacenter: "eu"},
		{addr: "4", datacenter: "us"},
		{addr: "5", datacenter: "us"},
	}

	return &topology{
		allNodes: dummyNodes,
	}
}

func TestRoundRobinPolicy(t *testing.T) {
	t.Parallel()
	topology := mockTopologyRoundRobin()
	testCases := []struct {
		name     string
		qi       QueryInfo
		expected []string
	}{
		{
			name:     "iteration 1",
			qi:       QueryInfo{topology: topology},
			expected: []string{"1", "2", "3", "4", "5"},
		},
		{
			name:     "iteration 2",
			qi:       QueryInfo{topology: topology},
			expected: []string{"2", "3", "4", "5", "1"},
		},
		{
			name:     "iteration 3",
			qi:       QueryInfo{topology: topology},
			expected: []string{"3", "4", "5", "1", "2"},
		},
		{
			name:     "iteration 4",
			qi:       QueryInfo{topology: topology},
			expected: []string{"4", "5", "1", "2", "3"},
		},
		{
			name:     "iteration 5",
			qi:       QueryInfo{topology: topology},
			expected: []string{"5", "1", "2", "3", "4"},
		},
		{
			name:     "iteration 6",
			qi:       QueryInfo{topology: topology},
			expected: []string{"1", "2", "3", "4", "5"},
		},
	}

	policy := newRoundRobinPolicy()

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		it := policy.PlanIter(tc.qi)
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, addr := range tc.expected {
				if res := it().addr; res != addr {
					t.Fatalf("TestRoundRobinPolicy: in test case %#+v: got \"%s\" but expected \"%s\"", tc, res, addr)
				}
			}
			if it() != nil {
				t.Fatalf("TestRoundRobinPolicy: plan iter didn't return nil after making the whole cycle")
			}
		})
	}
}

func TestDCAwareRoundRobinPolicy(t *testing.T) {
	t.Parallel()
	topology := mockTopologyRoundRobin()
	testCases := []struct {
		name     string
		qi       QueryInfo
		expected []string
	}{
		{
			name:     "iteration 1",
			qi:       QueryInfo{topology: topology},
			expected: []string{"1", "2", "3", "4", "5"},
		},
		{
			name:     "iteration 2",
			qi:       QueryInfo{topology: topology},
			expected: []string{"2", "3", "1", "5", "4"},
		},
		{
			name:     "iteration 3",
			qi:       QueryInfo{topology: topology},
			expected: []string{"3", "1", "2", "4", "5"},
		},
		{
			name:     "iteration 4",
			qi:       QueryInfo{topology: topology},
			expected: []string{"1", "2", "3", "5", "4"},
		},
	}

	policy := newDCAwareRoundRobin("eu")

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		it := policy.PlanIter(tc.qi)
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, addr := range tc.expected {
				if res := it().addr; res != addr {
					t.Fatalf("TestDCAwareRoundRobinPolicy: in test case %#+v: got \"%s\" but expected \"%s\"", tc, res, addr)
				}
			}
			if it() != nil {
				t.Fatalf("TestDCAwareRoundRobinPolicy: plan iter didn't return nil after making the whole cycle")
			}
		})
	}
}

// mockTopologyTokenAwareSimpleStrategy creates cluster topology with info about 3 nodes living in the same datacenter.
// Ring field is populated as follows:
// ring tokens:            50 100 150 200 250 300 400 500
// corresponding node ids: 2  1   2   3   1   2   3   1
// keyspaces:
// names:       "rf2"  "rf3"
// strategies:  simple simple
// rep factors: 2      3
func mockTopologyTokenAwareSimpleStrategy() *topology {
	dummyNodes := []*Node{
		{addr: "1", datacenter: "waw"},
		{addr: "2", datacenter: "waw"},
		{addr: "3", datacenter: "waw"},
	}
	ring := *btree.New(2)

	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[1], token: Token{value: 50}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[0], token: Token{value: 100}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[1], token: Token{value: 150}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[2], token: Token{value: 200}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[0], token: Token{value: 250}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[1], token: Token{value: 300}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[2], token: Token{value: 400}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[0], token: Token{value: 500}})

	ks := map[string]keyspace{
		"rf2": {strategy: strategy{stratType: simple, rf: 2}},
		"rf3": {strategy: strategy{stratType: simple, rf: 3}},
	}

	return &topology{
		allNodes:  dummyNodes,
		ring:      ring,
		keyspaces: ks,
	}
}

func TestTokenAwareSimpleStrategyPolicy(t *testing.T) {
	t.Parallel()
	topology := mockTopologyTokenAwareSimpleStrategy()
	testCases := []struct {
		name     string
		qi       QueryInfo
		expected []string
	}{
		{
			name:     "replication factor = 2",
			qi:       QueryInfo{token: &Token{value: 160}, topology: topology, ksName: "rf2"},
			expected: []string{"3", "1"},
		},
		{
			name:     "replication factor = 3",
			qi:       QueryInfo{token: &Token{value: 60}, topology: topology, ksName: "rf3"},
			expected: []string{"1", "2", "3"},
		},
		{
			name:     "token value equal to the one in the ring",
			qi:       QueryInfo{token: &Token{value: 500}, topology: topology, ksName: "rf3"},
			expected: []string{"1", "2", "3"},
		},
	}

	policy := newTokenAwarePolicy(dummyWrapper{})

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		it := policy.PlanIter(tc.qi)
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, addr := range tc.expected {
				if res := it().addr; res != addr {
					t.Fatalf("TestTokenAwareSimpleStrategyPolicy: in test case %#+v: got \"%s\" but expected \"%s\"", tc, res, addr)
				}
			}
			if it() != nil {
				t.Fatalf("TestTokenAwareSimpleStrategyPolicy: plan iter didn't return nil after making the whole cycle")
			}
		})
	}
}

// mockTopologyTokenAwareNetworkStrategy creates cluster topology with info about 8 nodes living in two different datacenters.
//
// ring field is populated as follows:
// ring tokens:            50 100 150 200 250 300 400 500 510
// corresponding node ids: 1  5   2   1   6   4   8   7   3
//
// datacenter:       waw
// nodes in rack r1: 1 2
// nodes in rack r2: 3 4
//
// datacenter:       her
// nodes in rack r3: 5 6
// nodes in rack r4: 7 8
//
// keyspace:
// name: "waw/her"
// strategy: network topology
// replication factors: waw: 2 her: 3
func mockTopologyTokenAwareNetworkStrategy() *topology {
	dummyNodes := []*Node{
		{addr: "1", datacenter: "waw", rack: "r1"},
		{addr: "2", datacenter: "waw", rack: "r1"},
		{addr: "3", datacenter: "waw", rack: "r2"},
		{addr: "4", datacenter: "waw", rack: "r2"},
		{addr: "5", datacenter: "her", rack: "r3"},
		{addr: "6", datacenter: "her", rack: "r3"},
		{addr: "7", datacenter: "her", rack: "r4"},
		{addr: "8", datacenter: "her", rack: "r4"},
	}
	dcs := RacksInDCMap{"waw": 2, "her": 2}
	ring := *btree.New(2)

	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[0], token: Token{value: 50}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[4], token: Token{value: 100}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[1], token: Token{value: 150}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[0], token: Token{value: 200}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[5], token: Token{value: 250}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[3], token: Token{value: 300}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[7], token: Token{value: 400}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[6], token: Token{value: 500}})
	ring.ReplaceOrInsert(RingEntry{node: dummyNodes[2], token: Token{value: 510}})

	ks := map[string]keyspace{
		"waw/her": {strategy: strategy{stratType: networkTopology, dcRF: map[string]repFactor{"waw": 2, "her": 3}}},
	}

	return &topology{
		racksInDC: dcs,
		allNodes:  dummyNodes,
		ring:      ring,
		keyspaces: ks,
	}
}

func TestTokenAwareNetworkStrategyPolicy(t *testing.T) {
	t.Parallel()
	topology := mockTopologyTokenAwareNetworkStrategy()
	testCases := []struct {
		name     string
		qi       QueryInfo
		expected []string
	}{
		{
			name: "\"waw\" dc with rf = 2, \"her\" dc with rf = 3",
			qi: QueryInfo{
				token:    &Token{value: 0},
				topology: topology,
				ksName:   "waw/her",
			},
			expected: []string{"1", "5", "6", "4", "8"},
		},
	}

	policy := newTokenAwarePolicy(dummyWrapper{})

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		it := policy.PlanIter(tc.qi)
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, addr := range tc.expected {
				if res := it().addr; res != addr {
					t.Fatalf("TestTokenAwareNetworkStrategyPolicy: in test case %#+v: got \"%s\" but expected \"%s\"", tc, res, addr)
				}
			}
			if it() != nil {
				t.Fatalf("TestTokenAwareNetworkStrategyPolicy: plan iter didn't return nil after making the whole cycle")
			}
		})
	}
}

type dummyWrapper struct{}

func (d dummyWrapper) PlanIter(qi QueryInfo) func() *Node {
	return d.WrapPlan(qi.topology.allNodes)
}

func (d dummyWrapper) WrapPlan(plan []*Node) func() *Node {
	counter := 0
	return func() *Node {
		if counter == len(plan) {
			return nil
		}

		defer func() { counter++ }()
		return plan[counter]
	}
}
