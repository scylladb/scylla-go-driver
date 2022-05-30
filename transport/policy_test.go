package transport

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

// Round-Robin tests can't be run in parallel because
// we have to know current number of iterations for testing result.

// mockTopologyRoundRobin creates cluster topology with info about 5 nodes living in 2 different datacenters.
func mockTopologyRoundRobin() *Topology {
	dummyNodes := []*Node{
		{addr: "1", datacenter: "eu"},
		{addr: "2", datacenter: "eu"},
		{addr: "3", datacenter: "eu"},
		{addr: "4", datacenter: "us"},
		{addr: "5", datacenter: "us"},
	}

	return &Topology{
		nodes: dummyNodes,
	}
}

func TestRoundRobinPolicy(t *testing.T) { //nolint:paralleltest // Can't run in parallel.
	testCases := []struct {
		name     string
		expected []string
	}{
		{
			name:     "iteration 1",
			expected: []string{"1", "2", "3", "4", "5"},
		},
		{
			name:     "iteration 2",
			expected: []string{"2", "3", "4", "5", "1"},
		},
		{
			name:     "iteration 3",
			expected: []string{"3", "4", "5", "1", "2"},
		},
		{
			name:     "iteration 4",
			expected: []string{"4", "5", "1", "2", "3"},
		},
		{
			name:     "iteration 5",
			expected: []string{"5", "1", "2", "3", "4"},
		},
		{
			name:     "iteration 6",
			expected: []string{"1", "2", "3", "4", "5"},
		},
	}

	policy := NewRoundRobinPolicy().New(mockTopologyRoundRobin(), Strategy{})

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			qi := policy.NewQueryInfo()
			for j := range tc.expected {
				if res := policy.Iter(qi, j).addr; res != tc.expected[j] {
					t.Fatalf("TestRoundRobinPolicy: got \"%s\" but expected \"%s\"", res, tc.expected[j])
				}
			}
			if policy.Iter(qi, len(tc.expected)) != nil {
				t.Fatalf("TestRoundRobinPolicy: plan iter didn't return nil after making the whole cycle")
			}
		})
	}
}

func TestDCAwareRoundRobinPolicy(t *testing.T) { //nolint:paralleltest // Can't run in parallel.
	testCases := []struct {
		name     string
		expected []string
	}{
		{
			name:     "iteration 1",
			expected: []string{"1", "2", "3", "4", "5"},
		},
		{
			name:     "iteration 2",
			expected: []string{"2", "3", "1", "5", "4"},
		},
		{
			name:     "iteration 3",
			expected: []string{"3", "1", "2", "4", "5"},
		},
		{
			name:     "iteration 4",
			expected: []string{"1", "2", "3", "5", "4"},
		},
	}

	policy := NewDCAwareRoundRobinPolicy("eu").New(mockTopologyRoundRobin(), Strategy{})

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			qi := policy.NewQueryInfo()
			for j := range tc.expected {
				if res := policy.Iter(qi, j).addr; res != tc.expected[j] {
					t.Fatalf("TestDCAwareRoundRobinPolicy: got \"%s\" but expected \"%s\"", res, tc.expected[j])
				}
			}
			if policy.Iter(qi, len(tc.expected)) != nil {
				t.Fatalf("TestDCAwareRoundRobinPolicy: plan iter didn't return nil after making the whole cycle")
			}
		})
	}
}

/*
	mockTopologyTokenAwareSimpleStrategy creates cluster topology with info about 4 nodes living in the same datacenter.

	Ring field is populated as follows:
	ring tokens:            50 100 150 200 250 300 400 500
	corresponding node ids: 2  1   4   3   1   2   3   4
*/
func mockTopologyTokenAwareSimpleStrategy() *Topology {
	dummyNodes := []*Node{
		{hostID: frame.UUID{1}, addr: "1", datacenter: "waw"},
		{hostID: frame.UUID{2}, addr: "2", datacenter: "waw"},
		{hostID: frame.UUID{3}, addr: "3", datacenter: "waw"},
		{hostID: frame.UUID{4}, addr: "4", datacenter: "waw"},
	}
	ring := []RingEntry{
		{node: dummyNodes[1], token: 50},
		{node: dummyNodes[0], token: 100},
		{node: dummyNodes[3], token: 150},
		{node: dummyNodes[2], token: 200},
		{node: dummyNodes[0], token: 250},
		{node: dummyNodes[1], token: 300},
		{node: dummyNodes[2], token: 400},
		{node: dummyNodes[3], token: 500},
	}

	return &Topology{
		nodes: dummyNodes,
		ring:  ring,
	}
}

func TestTokenAwareSimpleStrategyPolicy(t *testing.T) { //nolint:paralleltest // Not necessary in simple strategy unit test.
	testCases := []struct {
		name     string
		token    Token
		stg      Strategy
		expected []string
	}{
		{
			name:     "replication factor = 2",
			token:    160,
			stg:      Strategy{class: simpleStrategy, rf: 2},
			expected: []string{"3", "1"},
		},
		{
			name:     "replication factor = 3",
			token:    60,
			stg:      Strategy{class: simpleStrategy, rf: 3},
			expected: []string{"1", "4", "3"},
		},
		{
			name:     "token value equal to the one in the ring",
			token:    500,
			stg:      Strategy{class: simpleStrategy, rf: 3},
			expected: []string{"4", "2", "1"},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			policy := NewSimpleTokenAwarePolicy(NewDummyWrapper()).New(mockTopologyTokenAwareSimpleStrategy(), tc.stg)
			qi := policy.NewTokenAwareQueryInfo(tc.token)
			for j := range tc.expected {
				if res := policy.Iter(qi, j).addr; res != tc.expected[j] {
					t.Fatalf("TestTokenAwareSimpleStrategyPolicy: got \"%s\" but expected \"%s\"", res, tc.expected[j])
				}
			}
			if policy.Iter(qi, len(tc.expected)) != nil {
				t.Fatalf("TestTokenAwareSimpleStrategyPolicy: plan iter didn't return nil after making the whole cycle")
			}
		})
	}
}

/*
	mockTopologyTokenAwareNetworkStrategy creates cluster topology with info about 8 nodes
	living in two different datacenters.

	Ring field is populated as follows:
	ring tokens:            50 100 150 200 250 300 400 500 510
	corresponding node ids: 1  5   2   1   6   4   8   7   3

	Datacenter:       waw
	nodes in rack r1: 1 2
	nodes in rack r2: 3 4

	Datacenter:       her
	nodes in rack r3: 5 6
	nodes in rack r4: 7 8
*/
func mockTopologyTokenAwareNetworkStrategy() *Topology {
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
	// TODO: is this a proper Scylla ring?
	ring := []RingEntry{
		{node: dummyNodes[0], token: 50},
		{node: dummyNodes[4], token: 100},
		{node: dummyNodes[1], token: 150},
		{node: dummyNodes[0], token: 200},
		{node: dummyNodes[5], token: 250},
		{node: dummyNodes[3], token: 300},
		{node: dummyNodes[7], token: 400},
		{node: dummyNodes[6], token: 500},
		{node: dummyNodes[2], token: 510},
	}

	return &Topology{
		nodes: dummyNodes,
		ring:  ring,
	}
}

func TestTokenAwareNetworkStrategyPolicy(t *testing.T) { //nolint:paralleltest // Not necessary in simple strategy unit test.
	testCases := []struct {
		name     string
		token    Token
		stg      Strategy
		expected []string
	}{
		{
			name:     "'waw' dc with rf = 2, 'her' dc with rf = 3",
			token:    0,
			stg:      Strategy{class: networkTopologyStrategy, dcRF: dcRFMap{"waw": 2, "her": 3}},
			expected: []string{"1", "5", "6", "4", "8"},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			policy := NewNetworkTopologyTokenAwarePolicy(NewDummyWrapper()).New(mockTopologyTokenAwareNetworkStrategy(), tc.stg)
			qi := policy.NewTokenAwareQueryInfo(tc.token)
			for j := range tc.expected {
				if res := policy.Iter(qi, j).addr; res != tc.expected[j] {
					t.Fatalf("TestTokenAwareSimpleStrategyPolicy: got \"%s\" but expected \"%s\"", res, tc.expected[j])
				}
			}
			if policy.Iter(qi, len(tc.expected)) != nil {
				t.Fatalf("TestTokenAwareSimpleStrategyPolicy: plan iter didn't return nil after making the whole cycle")
			}
		})
	}
}

type dummyWrapper struct{}

func (p *dummyWrapper) New(_ *Topology, _ Strategy) HostSelectionPolicy {
	return &dummyWrapper{}
}

func NewDummyWrapper() *dummyWrapper {
	return &dummyWrapper{}
}

func (p *dummyWrapper) Iter(_ QueryInfo, _ int) *Node {
	return nil
}

func (p *dummyWrapper) WrapIter(reps []*Node, idx, _ int) *Node {
	if len(reps) <= idx {
		return nil
	}
	return reps[idx]
}

func (p *dummyWrapper) NewQueryInfo() QueryInfo {
	return QueryInfo{
		tokenAwareness: false,
	}
}

func (p *dummyWrapper) NewTokenAwareQueryInfo(t Token) QueryInfo {
	return QueryInfo{
		tokenAwareness: true,
		token:          t,
	}
}
