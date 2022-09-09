package transport

import (
	"testing"

	"github.com/scylladb/scylla-go-driver/frame"
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
		Nodes: dummyNodes,
	}
}

func mockCluster(t *topology, ks, localDC string) *Cluster {
	c := Cluster{}
	t.localDC = localDC

	if k, ok := t.keyspaces[ks]; ok {
		t.policyInfo.Preprocess(t, k)
	} else {
		t.policyInfo.Preprocess(t, keyspace{})
	}
	c.setTopology(t)

	return &c
}

func TestRoundRobinPolicy(t *testing.T) { //nolint:paralleltest // Can't run in parallel.
	top := mockTopologyRoundRobin()
	c := mockCluster(top, "", "")

	testCases := []struct {
		name     string
		qi       QueryInfo
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

	policy := NewTokenAwarePolicy("")
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		qi := c.NewQueryInfo()
		t.Run(tc.name, func(t *testing.T) {
			for offset, addr := range tc.expected {
				if res := policy.Node(qi, offset).addr; res != addr {
					t.Fatalf("TestRoundRobinPolicy: in test case %#+v: got \"%s\" but expected \"%s\"", tc, res, addr)
				}
			}
			if policy.Node(qi, len(tc.expected)) != nil {
				t.Fatalf("TestRoundRobinPolicy: plan iter didn't return nil after making the whole cycle")
			}
		})
	}
}

func TestDCAwareRoundRobinPolicy(t *testing.T) { //nolint:paralleltest // Can't run in parallel.
	top := mockTopologyRoundRobin()
	testCases := []struct {
		name     string
		qi       QueryInfo
		expected []string
	}{
		{
			name:     "iteration 1",
			qi:       QueryInfo{topology: top},
			expected: []string{"1", "2", "3", "4", "5"},
		},
		{
			name:     "iteration 2",
			qi:       QueryInfo{topology: top},
			expected: []string{"2", "3", "1", "5", "4"},
		},
		{
			name:     "iteration 3",
			qi:       QueryInfo{topology: top},
			expected: []string{"3", "1", "2", "4", "5"},
		},
		{
			name:     "iteration 4",
			qi:       QueryInfo{topology: top},
			expected: []string{"1", "2", "3", "5", "4"},
		},
	}

	policy := NewTokenAwarePolicy("eu")
	c := mockCluster(top, "", "eu")

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		qi := c.NewQueryInfo()
		t.Run(tc.name, func(t *testing.T) {
			for offset, addr := range tc.expected {
				if res := policy.Node(qi, offset).addr; res != addr {
					t.Fatalf("TestDCAwareRoundRobinPolicy: in test case %#+v: got \"%s\" but expected \"%s\"", tc, res, addr)
				}
			}
			if policy.Node(qi, len(tc.expected)) != nil {
				t.Fatalf("TestDCAwareRoundRobinPolicy: plan iter didn't return nil after making the whole cycle")
			}
		})
	}
}

/*
mockTopologyTokenAwareSimpleStrategy creates cluster topology with info about 3 nodes living in the same datacenter.

Ring field is populated as follows:
ring tokens:            50 100 150 200 250 300 400 500
corresponding node ids: 2  1   2   3   1   2   3   1

Keyspaces:
names:       "rf2"  "rf3"
strategies:  simple simple
rep factors: 2      3.
*/
func mockTopologyTokenAwareSimpleStrategy() *topology {
	dummyNodes := []*Node{
		{hostID: frame.UUID{1}, addr: "1", datacenter: "waw"},
		{hostID: frame.UUID{2}, addr: "2", datacenter: "waw"},
		{hostID: frame.UUID{3}, addr: "3", datacenter: "waw"},
	}
	ring := []RingEntry{
		{node: dummyNodes[1], token: 50},
		{node: dummyNodes[0], token: 100},
		{node: dummyNodes[1], token: 150},
		{node: dummyNodes[2], token: 200},
		{node: dummyNodes[0], token: 250},
		{node: dummyNodes[1], token: 300},
		{node: dummyNodes[2], token: 400},
		{node: dummyNodes[0], token: 500},
	}

	ks := ksMap{
		"rf2": {strategy: strategy{class: simpleStrategy, rf: 2}},
		"rf3": {strategy: strategy{class: simpleStrategy, rf: 3}},
	}

	return &topology{
		Nodes: dummyNodes,
		policyInfo: policyInfo{
			ring: ring,
		},
		keyspaces: ks,
	}
}

func TestTokenAwareSimpleStrategyPolicy(t *testing.T) { //nolint:paralleltest // Not necessary in simple strategy unit test.
	top := mockTopologyTokenAwareSimpleStrategy()
	testCases := []struct {
		name     string
		keyspace string
		token    Token
		qi       QueryInfo
		expected []string
	}{
		{
			name:     "replication factor = 2",
			keyspace: "rf2",
			token:    160,
			expected: []string{"3", "1"},
		},
		{
			name:     "replication factor = 3",
			keyspace: "rf3",
			token:    60,
			expected: []string{"1", "2", "3"},
		},
		// TODO mmt: use realistic mock ring
		//{
		//	name: "token value equal to the one in the ring",
		//	qi: QueryInfo{
		//		tokenAwareness: true,
		//		token:          500,
		//		topology:       top,
		//		strategy:       top.keyspaces["rf3"].strategy,
		//	},
		//	expected: []string{"1", "2", "3"},
		// },
	}

	policy := NewTokenAwarePolicy("")

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		c := mockCluster(top, tc.keyspace, "")
		qi, err := c.NewTokenAwareQueryInfo(tc.token, tc.keyspace)
		if err != nil {
			t.Fatal(err)
		}

		t.Run(tc.name, func(t *testing.T) {
			for offset, addr := range tc.expected {
				if res := policy.Node(qi, offset).addr; res != addr {
					t.Fatalf("TestTokenAwareSimpleStrategyPolicy: in test case %#+v: got \"%s\" but expected \"%s\"", tc, res, addr)
				}
			}
			if policy.Node(qi, len(tc.expected)) != nil {
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

Keyspace:         "waw/her"
strategy: network topology
replication factors: waw: 2 her: 3.
*/
func mockTopologyTokenAwareDCAwareStrategy() *topology {
	dummyNodes := []*Node{
		{hostID: frame.UUID{1}, addr: "1", datacenter: "waw", rack: "r1"},
		{hostID: frame.UUID{2}, addr: "2", datacenter: "waw", rack: "r1"},
		{hostID: frame.UUID{3}, addr: "3", datacenter: "waw", rack: "r2"},
		{hostID: frame.UUID{4}, addr: "4", datacenter: "waw", rack: "r2"},
		{hostID: frame.UUID{5}, addr: "5", datacenter: "her", rack: "r3"},
		{hostID: frame.UUID{6}, addr: "6", datacenter: "her", rack: "r3"},
		{hostID: frame.UUID{7}, addr: "7", datacenter: "her", rack: "r4"},
		{hostID: frame.UUID{8}, addr: "8", datacenter: "her", rack: "r4"},
	}
	dcs := dcRacksMap{"waw": 2, "her": 2}
	ring := Ring{
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

	ks := ksMap{
		"waw/her": {strategy: strategy{class: networkTopologyStrategy, dcRF: dcRFMap{"waw": 2, "her": 3}}},
	}

	return &topology{
		dcRacks:    dcs,
		Nodes:      dummyNodes,
		policyInfo: policyInfo{ring: ring},
		keyspaces:  ks,
	}
}

func TestTokenAwareNetworkStrategyPolicy(t *testing.T) { //nolint:paralleltest // Not necessary in simple strategy unit test.
	top := mockTopologyTokenAwareDCAwareStrategy()
	testCases := []struct {
		name     string
		keyspace string
		localDC  string
		token    Token
		qi       QueryInfo
		expected []string
	}{
		{
			name:     "'waw' dc with rf = 2, 'her' dc with rf = 3",
			keyspace: "waw/her",
			localDC:  "waw",
			token:    0,
			// not {"1", "2", "5", "6", "8"} as node "2" is on the same rack as "1"
			expected: []string{"1", "4", "5", "6", "8"},
		},
	}

	for i := 0; i < len(testCases); i++ {
		policy := NewTokenAwarePolicy("waw")
		tc := testCases[i]
		c := mockCluster(top, tc.keyspace, tc.localDC)
		qi, err := c.NewTokenAwareQueryInfo(tc.token, tc.keyspace)
		if err != nil {
			t.Fatal(err)
		}

		t.Run(tc.name, func(t *testing.T) {
			for offset, addr := range tc.expected {
				if res := policy.Node(qi, offset).addr; res != addr {
					t.Fatalf("TestTokenAwareSimpleStrategyPolicy: in test case %#+v: got \"%s\" but expected \"%s\"", tc, res, addr)
				}
			}
			if policy.Node(qi, len(tc.expected)) != nil {
				t.Fatalf("TestTokenAwareNetworkStrategyPolicy: plan iter didn't return nil after making the whole cycle")
			}
		})
	}
}
