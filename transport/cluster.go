package transport

import (
	"fmt"
	"log"
	"net"
	"time"

	"scylla-go-driver/frame"
	. "scylla-go-driver/frame/response"

	"go.uber.org/atomic"
)

type (
	PeerMap = map[string]*Node

	refreshHandler chan struct{}
)

type Cluster struct {
	peers         atomic.Value // PeerMap
	control       *Conn
	cfg           ConnConfig
	events        responseHandler
	handledEvents []frame.EventType // This will probably be moved to config.
	knownHosts    []string
	refresher     refreshHandler

	// Yet to be used.
	// ring        map[Token]*Node
	// dataCenters map[string]*DataCenter
}

// type dataCenter struct {
// 	nodes   []Node
// 	rackCnt uint // Type?
// }

const (
	eventChanSize   = 32
	refreshChanSize = 32
)

// NewCluster also creates control connection and starts handling events and refreshing topology.
func NewCluster(cfg ConnConfig, e []frame.EventType, hosts ...string) (*Cluster, error) {
	if len(hosts) == 0 {
		return nil, fmt.Errorf("at least one host is required to create cluster")
	}

	k := make([]string, len(hosts))
	copy(k, hosts)

	c := &Cluster{
		cfg:           cfg,
		events:        make(responseHandler, eventChanSize),
		handledEvents: e,
		knownHosts:    k,
		refresher:     make(refreshHandler, refreshChanSize),
	}
	c.setPeers(PeerMap{})

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
	for _, addr := range c.knownHosts {
		conn, err := OpenConn(addr, nil, c.cfg)
		if err == nil {
			h := func(r response) {
				select {
				case c.events <- r:
				default:
					log.Printf("event dropped due to full event channel: %#+v", r)
				}
			}

			if err := conn.register(h, c.handledEvents...); err == nil {
				c.control = conn
				go c.handleEvents()
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
	// TODO: should NewControl just log.Fatalf instead of returning an error?
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
	// If node is present in both maps we can update already created node
	// instead of creating new one from the scratch.
	old := c.Peers()
	m := PeerMap{}
	for _, v := range rows {
		addr := net.IP(v[nodeAddr]).String() // TODO: add parsing for columns in query result.
		n := &Node{
			addr:       addr,
			datacenter: string(v[nodeDC]),
			rack:       string(v[nodeRack]),
			tokens:     v[nodeTokens],
		}

		if node, ok := old[addr]; ok {
			n.pool = node.pool
			n.setStatus(node.Status())
		} else {
			if pool, err := NewConnPool(addr, c.cfg); err != nil {
				n.setStatus(statusDown)
			} else {
				n.setStatus(statusUP)
				n.pool = pool
			}
		}
		m[addr] = n
	}

	for k, v := range old {
		if _, ok := m[k]; v.pool != nil && !ok {
			v.pool.Close()
		}
	}
	c.setPeers(m)
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

// Indexes of columns returned from refresh topology queries.
const (
	nodeAddr   = 0
	nodeDC     = 1
	nodeRack   = 2
	nodeTokens = 3
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

func (c *Cluster) Peers() PeerMap {
	return c.peers.Load().(PeerMap)
}

func (c *Cluster) setPeers(m PeerMap) {
	c.peers.Store(m)
}

func (c *Cluster) handleEvents() {
	for {
		res := <-c.events
		if res.Err == closeCluster {
			// We close channel here since it's the only place where we are sending messages on it.
			close(c.refresher)
			return
		}
		if res.Err != nil {
			log.Printf("received event response with error: %v", res.Err)
			continue
		}

		switch v := res.Response.(type) {
		case *TopologyChange:
			c.handleTopologyChange(v)
		case *StatusChange:
			c.handleStatusChange(v)
		case *SchemaChange:
			// TODO: add schema change.
		default:
			log.Printf("unsupported event type: %v", res.Response)
		}
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
		log.Printf("unknown node %s recives status change: %+#v", addr, v)
		c.RequestRefresh()
	}
}

// RequestRefresh notifies cluster that it should update topology.
func (c *Cluster) RequestRefresh() {
	c.refresher <- struct{}{}
}

const refreshInterval = 60 * time.Second

// loop handles refresh topology requests.
func (c *Cluster) loop() {
	ticker := time.NewTimer(refreshInterval)
	for {
		select {
		case _, ok := <-c.refresher:
			if ok {
				c.tryRefresh()
				ticker.Stop()
				ticker.Reset(refreshInterval)
			} else {
				return
			}
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
		log.Printf("refresh topology: %s", err.Error())
		c.control.Close()
		if err := c.NewControl(); err != nil {
			c.Close()
			log.Fatalf("reopen control connection: %v", err)
		}
		if err := c.refreshTopology(); err != nil {
			c.Close()
			log.Fatalf("can't refresh topology after reopening control connetion: %v", err)
		}
	}
}

var closeCluster = fmt.Errorf("close cluster")

func (c *Cluster) Close() {
	c.control.Close()
	c.events <- response{Err: closeCluster}
	m := c.Peers()

	for _, v := range m {
		if v.pool != nil {
			v.pool.Close()
		}
	}
}
