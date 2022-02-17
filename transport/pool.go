package transport

import (
	"errors"
	"runtime"
	"scylla-go-driver/frame"
	. "scylla-go-driver/frame/request"
	. "scylla-go-driver/frame/response"
	"sync"
	"time"
)

type ConnPool struct {
	conns []*Conn
	mu    sync.Mutex
}

type PoolRefiller struct {
	address    string
	connConfig ConnConfig
	connPool   ConnPool

	activeShards uint16
	nrShards     uint16

	connOptions      frame.ScyllaSupported
	errRcv           chan uint16
	errorSinceRefill bool
	currentDelay     time.Duration
}

const (
	minFillBackoff        = 50 * time.Millisecond
	maxFillBackoff        = 10 * time.Second
	fillBackoffMultiplier = 2
)

func (p *PoolRefiller) loop() { // nolint:unused // This will be used.
	runtime.LockOSThread()

	p.currentDelay = minFillBackoff
	ticker := time.NewTicker(p.currentDelay)
	for {
		select {
		case <-ticker.C:
			p.fill()
		case nr := <-p.errRcv:
			p.removeConnection(nr)
		}

		if p.needsFilling() {
			if p.errorSinceRefill {
				p.onFillError()
			} else {
				p.currentDelay = minFillBackoff
			}

			ticker.Reset(p.currentDelay)
		} else {
			ticker.Stop()
		}
	}
}

func (p *PoolRefiller) onFillError() {
	if maxFillBackoff > p.currentDelay*fillBackoffMultiplier {
		p.currentDelay *= fillBackoffMultiplier
	} else {
		p.currentDelay = maxFillBackoff
	}
}

func (p *PoolRefiller) needsFilling() bool { // nolint:unused // This will be used.
	return p.activeShards < p.nrShards
}

func (p *PoolRefiller) removeConnection(nr uint16) { // nolint:unused // This will be used.
	p.connPool.mu.Lock()
	err := p.connPool.conns[nr].conn.Close()
	p.errorSinceRefill = p.errorSinceRefill || (err != nil)
	p.connPool.conns[nr] = nil
	p.activeShards--
	p.connPool.mu.Unlock()
}

func (p *PoolRefiller) fill() {
	p.connPool.mu.Lock()
	if p.activeShards == 0 && p.nrShards == 0 {
		// First connection is needed to be done a bit differently,
		// without opening it to a specific shard to read frame.ScyllaSupported.
		conn, err := OpenConn(p.address, nil, p.connConfig, p.errRcv, 0)
		if err != nil {
			p.connPool.mu.Unlock()
			return
		}
		p.connOptions, err = getScyllaSupported(conn)
		if err != nil {
			p.connPool.mu.Unlock()
			return
		}

		p.nrShards = p.connOptions.NrShards
		p.connPool.conns = make([]*Conn, p.nrShards)
		p.connPool.conns[p.connOptions.Shard] = conn
		p.activeShards++
		// Adjust shard number for the fact that we passed it as 0.
		conn.shardNr = p.connOptions.Shard
	}

	n := p.connOptions.NrShards
	for i := uint16(0); i < n && p.activeShards < p.nrShards; i++ {
		if p.connPool.conns[i] != nil {
			continue
		}

		shard := ShardInfo{
			Shard:    i,
			NrShards: n,
		}

		conn, err := OpenShardConn(p.address, shard, p.connConfig, p.errRcv)
		if err == nil {
			p.connPool.conns[shard.Shard] = conn
			p.activeShards++
		} else {
			p.errorSinceRefill = true
		}
	}
	p.connPool.mu.Unlock()
}

func getScyllaSupported(conn *Conn) (frame.ScyllaSupported, error) {
	res, err := conn.sendRequest(&Options{}, false, false)
	if err != nil {
		return frame.ScyllaSupported{}, err
	}
	supp, ok := res.(*Supported)
	if !ok {
		return frame.ScyllaSupported{}, errors.New("couldn't cast interface to struct")
	}

	return supp.ParseScyllaSupported(), nil
}

func InitNodeConnPool(addr string, connConfig ConnConfig) *ConnPool { // nolint:unused // This will be used.
	pr := PoolRefiller{
		address:    addr,
		connConfig: connConfig,
	}

	go pr.loop()
	return &pr.connPool
}