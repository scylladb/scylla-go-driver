package transport

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"scylla-go-driver/frame"
	. "scylla-go-driver/frame/response"

	"go.uber.org/atomic"
)

type (
	PeerMap = map[string]*Node

	eventHandler   = responseHandler
	refreshHandler chan struct{}
)

type Cluster struct {
	peers         atomic.Value // PeerMap
	control       *Conn
	cfg           ConnConfig
	events        eventHandler
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

// NewCluster also creates control connection and starts handling events and refreshing topology.
func NewCluster(cfg ConnConfig, e []frame.EventType, hosts ...string) (*Cluster, error) {
	if len(hosts) < 1 {
		return nil, fmt.Errorf("at least one host is required to create cluster")
	}

	p := atomic.Value{}
	p.Store(PeerMap{})
	k := make([]string, len(hosts))
	copy(k, hosts)

	c := &Cluster{
		peers:         p,
		cfg:           cfg,
		handledEvents: e,
		knownHosts:    k,
	}

	if err := c.NewControl(); err != nil {
		return nil, fmt.Errorf("creating cluster: %w", err)
	}
	if err := c.refreshTopology(); err != nil {
		return nil, fmt.Errorf("creating cluster: %w", err)
	}

	go c.loop()
	return c, nil
}

func (c *Cluster) NewControl() error {
	log.Printf("opening control connection")
	for _, v := range c.knownHosts {
		control, err := OpenConn(v, nil, c.cfg)
		if err == nil {
			if events, err := control.registerEvents(c.handledEvents); err == nil {
				c.control = control
				c.events = events

				go c.handleEvents()
				return nil
			}
		}
		if control != nil {
			control.Close()
		}
	}
	return fmt.Errorf("couldn't open control connection to any known host")
}

// refreshTopology creates new PeerMap filled with the result of both localQuery and peerQuery.
// The old map is replaced with the new one atomically to prevent dirty reads.
func (c *Cluster) refreshTopology() error {
	log.Printf("refreshing topology")
	rows, err := c.getAllNodesInfo()
	if err != nil {
		return fmt.Errorf("refreh topology: %w", err)
	}
	// If node is present in both maps we can update already created node
	// instead of creating new one from the scratch.
	old := c.GetPeers()
	m := PeerMap{}
	for _, v := range rows {
		addr := net.IP(v[nodeAddr]).String()

		if node, ok := old[addr]; ok {
			node.tokens = v[nodeTokens]
			m[addr] = node
		} else {
			n := &Node{
				addr:       addr,
				datacenter: string(v[nodeDC]),
				rack:       string(v[nodeRack]),
				tokens:     v[nodeTokens],
			}
			if pool, err := NewConnPool(appendDefaultPort(addr), c.cfg); err != nil {
				n.status.Store(statusDown)
			} else {
				n.status.Store(statusUP)
				n.pool = pool
			}
			m[addr] = n
		}
	}

	for k, v := range old {
		if _, ok := m[k]; !ok {
			v.pool.Close()
		}
	}
	c.SetPeers(m)
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
		return nil, fmt.Errorf("discovering peer topology: %w", err)
	}

	localRes, err := c.control.Query(localQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("discovering local topology: %w", err)
	}

	return append(peerRes.Rows, localRes.Rows[0]), nil
}

func (c *Cluster) GetPeers() PeerMap {
	return c.peers.Load().(PeerMap)
}

func (c *Cluster) SetPeers(m PeerMap) {
	c.peers.Store(m)
}

const defaultPort = 9042

func appendDefaultPort(addr string) string {
	return addr + ":" + strconv.Itoa(defaultPort)
}

const eventChanSize = 32

func (c *Cluster) handleEvents() {
	for {
		res := <-c.events
		if res.Err != nil {
			return
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
	log.Printf("handling node: %s topology change to: %s", v.Address.String(), v.Change)
	c.RequestRefresh()
}

func (c *Cluster) handleStatusChange(v *StatusChange) {
	log.Printf("handling node: %s status change to: %s", v.Address.String(), v.Status)
	m := c.GetPeers()
	addr := v.Address.String()
	if n, ok := m[addr]; ok {
		switch v.Status {
		case frame.Up:
			n.status.Store(statusUP)
		case frame.Down:
			n.status.Store(statusDown)
		default:
			log.Fatalf("status change not supported: %s", v.Status)
		}
	} else {
		log.Printf("node which status is being set to %s was not present in known topology: %s", v.Status, addr)
		c.RequestRefresh()
	}
}

// RequestRefresh notifies cluster that it should update topology.
// TODO: do we need some mechanism for notifying requester that refresh was successful?
func (c *Cluster) RequestRefresh() {
	c.refresher <- struct{}{}
}

const (
	refreshChanSize = 32
	refreshInterval = 60 * time.Second
)

// loop handles refresh topology requests.
func (c *Cluster) loop() {
	c.refresher = make(refreshHandler, refreshChanSize)
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
		log.Printf("failed to refresh topology: %s", err.Error())
		c.control.Close()
		if err = c.NewControl(); err != nil {
			c.StopCluster()
			log.Fatalf("cant't reopen control connection: %v", err)
		}
		if err = c.refreshTopology(); err != nil {
			c.StopCluster()
			log.Fatalf("can't refresh topology even after reopening control connetion: %v", err)
		}
	}
}

func (c *Cluster) StopRefresher() {
	close(c.refresher)
}

func (c *Cluster) StopCluster() {
	c.StopRefresher()
	// Closing control connection automatically ends handleEvents loop.
	c.control.Close()
	m := c.GetPeers()

	for _, v := range m {
		if v.pool != nil {
			v.pool.Close()
		}
	}
}