package transport

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"time"

	"go.uber.org/atomic"
)

var _poolCloseShard = -1

type ConnPool struct {
	nrShards     int
	msbIgnore    uint8
	conns        []atomic.Value
	idx          atomic.Uint64
	connClosedCh chan int // notification channel for when connection is closed
}

func NewConnPool(addr string, cfg ConnConfig) (*ConnPool, error) {
	r := PoolRefiller{
		addr: appendDefaultPort(addr),
		cfg:  cfg,
	}
	if err := r.initConnPool(); err != nil {
		return nil, err
	}

	go r.loop()

	return &r.pool, nil
}

const defaultPort = 19042 // TODO: change to 9042 after rebasing to main.

func appendDefaultPort(addr string) string {
	return addr + ":" + strconv.Itoa(defaultPort)
}

func (p *ConnPool) RandConn() *Conn {
	conn := p.loadConn(rand.Intn(p.nrShards))
	if conn != nil {
		return conn
	}
	return p.NextConn()
}

func (p *ConnPool) NextConn() *Conn {
	idx := p.idx.Add(1)
	for i := 0; i < p.nrShards; i++ {
		if conn := p.loadConn(int((idx + uint64(i)) % uint64(p.nrShards))); conn != nil {
			return conn
		}
	}
	return nil
}

func (p *ConnPool) Conn(token Token) *Conn {
	idx := p.shardOf(token)
	if conn := p.loadConn(idx); conn != nil {
		return conn
	}
	return p.RandConn()
}

func (p *ConnPool) shardOf(token Token) int {
	shards := uint64(p.nrShards)
	z := uint64(token.value+math.MinInt64) << p.msbIgnore
	lo := z & 0xffffffff
	hi := (z >> 32) & 0xffffffff
	mul1 := lo * shards
	mul2 := hi * shards
	sum := (mul1 >> 32) + mul2
	return int(sum >> 32)
}

func (p *ConnPool) storeConn(conn *Conn) {
	p.conns[conn.Shard()].Store(conn)
}

func (p *ConnPool) loadConn(shard int) *Conn {
	conn, _ := p.conns[shard].Load().(*Conn)
	return conn
}

func (p *ConnPool) clearConn(shard int) bool {
	conn, _ := p.conns[shard].Swap((*Conn)(nil)).(*Conn)
	return conn != nil
}

func (p *ConnPool) Close() {
	p.connClosedCh <- _poolCloseShard
}

// closeAll is called by PoolRefiller.
func (p *ConnPool) closeAll() {
	for i := range p.conns {
		if conn, ok := p.conns[i].Swap((*Conn)(nil)).(*Conn); ok {
			conn.Close()
		}
	}
}

type PoolRefiller struct {
	addr   string
	pool   ConnPool
	cfg    ConnConfig
	active int
}

func (r *PoolRefiller) initConnPool() error {
	conn, err := OpenConn(r.addr, nil, r.cfg)
	if err != nil {
		return err
	}

	s, err := conn.Supported()
	if err != nil {
		conn.Close()
		return fmt.Errorf("supported: %w", err)
	}
	ss := s.ScyllaSupported()

	r.pool = ConnPool{
		nrShards:     int(ss.NrShards),
		msbIgnore:    ss.MsbIgnore,
		conns:        make([]atomic.Value, int(ss.NrShards)),
		connClosedCh: make(chan int, int(ss.NrShards)+1),
	}
	conn.setOnClose(r.onConnClose)
	r.pool.storeConn(conn)
	r.active = 1

	return nil
}

func (r *PoolRefiller) onConnClose(conn *Conn) {
	select {
	case r.pool.connClosedCh <- conn.Shard():
	default:
		log.Printf("conn pool: ignoring conn %s close", conn)
	}
}

const fillBackoff = time.Second

func (r *PoolRefiller) loop() {
	r.fill()

	timer := time.NewTicker(fillBackoff)
	for {
		select {
		case <-timer.C:
			r.fill()
		case shard := <-r.pool.connClosedCh:
			if shard == _poolCloseShard {
				r.pool.closeAll()
				return
			}
			if r.pool.clearConn(shard) {
				r.active--
			}
			r.fill()
		}
	}
}

func (r *PoolRefiller) fill() {
	if !r.needsFilling() {
		return
	}

	si := ShardInfo{
		NrShards:  uint16(r.pool.nrShards),
		MsbIgnore: r.pool.msbIgnore,
	}

	for i := 0; i < r.pool.nrShards; i++ {
		if r.pool.loadConn(i) != nil {
			continue
		}

		si.Shard = uint16(i)
		conn, err := OpenShardConn(r.addr, si, r.cfg)
		if err != nil {
			log.Printf("failed to open shard conn: %s", err)
			continue
		}
		if conn.Shard() != i {
			log.Fatalf("opened conn to wrong shard: expected %d got %d", i, conn.Shard())
		}
		conn.setOnClose(r.onConnClose)
		r.pool.storeConn(conn)
		r.active++

		if !r.needsFilling() {
			return
		}
	}
}

func (r *PoolRefiller) needsFilling() bool {
	return r.active < r.pool.nrShards
}
