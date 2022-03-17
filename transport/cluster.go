package transport

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"scylla-go-driver/frame"
	. "scylla-go-driver/frame/response"

	"github.com/google/btree"
	"go.uber.org/atomic"
)

type (
	PeerMap      = map[string]*Node
	RacksInDCMap = map[string]int

	requestChan chan struct{}
)

type Cluster struct {
	topology          atomic.Value // *topology
	control           *Conn
	cfg               ConnConfig
	handledEvents     []frame.EventType // This will probably be moved to config.
	knownHosts        []string
	refreshChan       requestChan
	reopenControlChan requestChan
	closeChan         requestChan
}

type topology struct {
	peers     PeerMap
	racksInDC RacksInDCMap
	allNodes  []*Node
	ring      btree.BTree
}

// ringRange return slice of nodes (desirably of length cnt) holding data described by token.
// wanted function allows applying additional requirements for nodes to be taken.
func (t *topology) ringRange(token Token, cnt int, wanted func(*Node, []*Node) bool) []*Node {
	replicas := make([]*Node, 0, cnt)

	it := func(i btree.Item) bool {
		n := i.(RingEntry).node
		if wanted(n, replicas) {
			replicas = append(replicas, n)
		}
		return len(replicas) < cnt
	}

	re := RingEntry{token: token}
	t.ring.AscendGreaterOrEqual(re, it)
	if len(replicas) < cnt {
		t.ring.AscendLessThan(re, it)
	}

	return replicas
}

// NewCluster also creates control connection and starts handling events and refreshing topology.
func NewCluster(cfg ConnConfig, e []frame.EventType, hosts ...string) (*Cluster, error) {
	if len(hosts) == 0 {
		return nil, fmt.Errorf("at least one host is required to create cluster")
	}

	k := make([]string, len(hosts))
	copy(k, hosts)

	c := &Cluster{
		cfg:               cfg,
		handledEvents:     e,
		knownHosts:        k,
		refreshChan:       make(requestChan, 1),
		reopenControlChan: make(requestChan, 1),
		closeChan:         make(requestChan, 1),
	}
	c.setTopology(&topology{})

	if err := c.NewControl(); err != nil {
		return nil, fmt.Errorf("create control connection: %w", err)
	}
	if err := c.refreshTopology(); err != nil {
		return nil, fmt.Errorf("refresh topology: %w", err)
	}

	go c.loop()
	return c, nil
}

func (c *Cluster) NewControl() error {
	log.Printf("open control connection")
	c.control = nil
	for _, addr := range c.knownHosts {
		conn, err := OpenConn(addr, nil, c.cfg)
		if err == nil {
			if err := conn.register(c.handleEvent, c.handledEvents...); err == nil {
				c.control = conn
				return nil
			} else {
				log.Printf("open control connection: node %s failed to register for events: %v", addr, err)
			}
		} else {
			log.Printf("open control connection: node %s failed to connect: %v", addr, err)
		}
		if conn != nil {
			conn.Close()
		}
	}

	return fmt.Errorf("couldn't open control connection to any known host: %v", c.knownHosts)
}

// refreshTopology creates new PeerMap filled with the result of both localQuery and peerQuery.
// The old map is replaced with the new one atomically to prevent dirty reads.
func (c *Cluster) refreshTopology() error {
	log.Printf("refresh topology")
	rows, err := c.getAllNodesInfo()
	if err != nil {
		return fmt.Errorf("query info about nodes in cluster: %w", err)
	}
	// If node is present in both maps we can reuse its connection pool.
	old := c.Peers()
	peers := make(PeerMap)
	racksInDC := make(RacksInDCMap)
	seenRacks := make(map[string]bool) // Key is rack name appended to its dc name.
	all := make([]*Node, 0, len(old))
	ring := *btree.New(2)

	for _, r := range rows {
		n, err := cqlRowIntoNode(r)
		if err != nil {
			return err
		}

		if node, ok := old[n.addr]; ok {
			n.pool = node.pool
			n.setStatus(node.Status())
		} else {
			if pool, err := NewConnPool(n.addr, c.cfg); err != nil {
				n.setStatus(statusDown)
			} else {
				n.setStatus(statusUP)
				n.pool = pool
			}
		}

		peers[n.addr] = n
		all = append(all, n)
		if err := insertNodeTokens(n, r, ring); err != nil {
			return err
		}

		if _, ok := racksInDC[n.datacenter]; !ok {
			racksInDC[n.datacenter] = 1
		} else {
			rackID := n.datacenter + n.rack
			if _, ok := seenRacks[rackID]; !ok {
				seenRacks[rackID] = true
				racksInDC[n.datacenter]++
			}
		}
	}

	for k, v := range old {
		if _, ok := peers[k]; v.pool != nil && !ok {
			v.pool.Close()
		}
	}

	c.setTopology(&topology{
		peers:     peers,
		racksInDC: racksInDC,
		allNodes:  all,
		ring:      ring,
	})
	// Discards request that came to us while refreshing.
	select {
	case <-c.refreshChan:
	default:
	}
	return nil
}

var (
	peerQuery = Statement{
		Content:     "SELECT peer, data_center, rack, tokens FROM system.peers",
		Consistency: frame.ONE,
	}

	localQuery = Statement{
		Content:     "SELECT rpc_address, data_center, rack, tokens FROM system.local",
		Consistency: frame.ONE,
	}
)

func (c *Cluster) getAllNodesInfo() ([]frame.Row, error) {
	peerRes, err := c.control.Query(peerQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("discover peer topology: %w", err)
	}

	localRes, err := c.control.Query(localQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("discover local topology: %w", err)
	}

	return append(peerRes.Rows, localRes.Rows[0]), nil
}

// Indexes of columns returned from refresh topology query.
const (
	nodeAddr   = 0
	nodeDC     = 1
	nodeRack   = 2
	nodeTokens = 3
)

func cqlRowIntoNode(r frame.Row) (*Node, error) {
	addr, err := r[nodeAddr].AsIP()
	if err != nil {
		return nil, fmt.Errorf("addr column: %w", err)
	}
	dc, err := r[nodeDC].AsText()
	if err != nil {
		return nil, fmt.Errorf("datacenter column: %w", err)
	}
	rack, err := r[nodeRack].AsText()
	if err != nil {
		return nil, fmt.Errorf("rack column: %w", err)
	}
	return &Node{
		addr:       addr.String(),
		datacenter: dc,
		rack:       rack,
	}, nil
}

func insertNodeTokens(n *Node, r frame.Row, ring btree.BTree) error {
	if tokens, err := r[nodeTokens].AsTextSet(); err != nil {
		return err
	} else {
		for _, t := range tokens {
			if v, err := strconv.ParseInt(t, 10, 64); err != nil {
				return fmt.Errorf("couldn't parse token string: %v", err)
			} else {
				ring.ReplaceOrInsert(RingEntry{
					node:  n,
					token: Token{value: v},
				})
			}
		}
	}
	return nil
}

func (c *Cluster) Peers() PeerMap {
	return c.topology.Load().(*topology).peers
}

func (c *Cluster) RacksInDC() RacksInDCMap {
	return c.topology.Load().(*topology).racksInDC
}

func (c *Cluster) AllNodes() []*Node {
	return c.topology.Load().(*topology).allNodes
}

func (c *Cluster) setTopology(t *topology) {
	c.topology.Store(t)
}

// handleEvent creates function which is passed to control connection
// via registerEvents in order to handle events right away instead
// of registering handlers for them.
func (c *Cluster) handleEvent(r response) {
	if r.Err != nil {
		log.Printf("received event with error: %v", r.Err)
		c.RequestReopenControl()
		return
	}
	switch v := r.Response.(type) {
	case *TopologyChange:
		c.handleTopologyChange(v)
	case *StatusChange:
		c.handleStatusChange(v)
	case *SchemaChange:
		// TODO: add schema change.
	default:
		log.Printf("unsupported event type: %v", r.Response)
	}
}

func (c *Cluster) handleTopologyChange(v *TopologyChange) {
	log.Printf("handle topology change: %+#v", v)
	c.RequestRefresh()
}

func (c *Cluster) handleStatusChange(v *StatusChange) {
	log.Printf("handle status change: %+#v", v)
	m := c.Peers()
	addr := v.Address.String()
	if n, ok := m[addr]; ok {
		switch v.Status {
		case frame.Up:
			n.setStatus(statusUP)
		case frame.Down:
			n.setStatus(statusDown)
		default:
			log.Printf("status change not supported: %+#v", v)
		}
	} else {
		log.Printf("unknown node %s received status change: %+#v in topology %v", addr, v, m)
		c.RequestRefresh()
	}
}

const refreshInterval = 60 * time.Second

// loop handles cluster requests.
func (c *Cluster) loop() {
	ticker := time.NewTimer(refreshInterval)
	for {
		select {
		case <-c.refreshChan:
			c.tryRefresh()
			ticker.Stop()
			ticker.Reset(refreshInterval)
		case <-c.reopenControlChan:
			c.reopenControl()
		case <-c.closeChan:
			c.handleClose()
			return
		case <-ticker.C:
			c.tryRefresh()
			ticker.Reset(refreshInterval)
		}
	}
}

// tryRefresh refreshes cluster topology.
// In case of error tries to reopen control connection and tries again.
func (c *Cluster) tryRefresh() {
	if err := c.refreshTopology(); err != nil {
		log.Printf("refresh topology: %v", err)
		c.reopenControl()
		if err := c.refreshTopology(); err != nil {
			c.Close()
			log.Fatalf("can't refresh topology after reopening control connetion: %v", err)
		}
	}
}

func (c *Cluster) reopenControl() {
	log.Printf("reopen control connection")
	c.control.Close()
	if err := c.NewControl(); err != nil {
		c.Close()
		log.Fatalf("failed to reopen control connection: %v", err)
	}
	// Discards request that came to us while reopening.
	select {
	case <-c.reopenControlChan:
	default:
	}
}

func (c *Cluster) handleClose() {
	log.Printf("handle cluster close")
	if c.control != nil {
		c.control.Close()
	}
	m := c.Peers()
	for _, v := range m {
		if v.pool != nil {
			v.pool.Close()
		}
	}
}

func (c *Cluster) RequestRefresh() {
	log.Printf("requested to refresh cluster topology")
	select {
	case c.refreshChan <- struct{}{}:
	default:
	}
}

func (c *Cluster) RequestReopenControl() {
	log.Printf("requested to reopen control connection")
	select {
	case c.reopenControlChan <- struct{}{}:
	default:
	}
}

func (c *Cluster) Close() {
	log.Printf("requested to close cluster")
	select {
	case c.closeChan <- struct{}{}:
	default:
	}
}
