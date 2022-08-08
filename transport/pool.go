package transport

import (
	"fmt"
	"log"
	"math"
	"net"
	"time"

	"github.com/mmatczuk/scylla-go-driver/frame"
	. "github.com/mmatczuk/scylla-go-driver/frame/response"

	"go.uber.org/atomic"
)

const poolCloseShard = -1

type ConnPool struct {
	host         string
	nrShards     int
	msbIgnore    uint8
	conns        []atomic.Value
	connClosedCh chan int // notification channel for when connection is closed
	connObs      ConnObserver
}

func NewConnPool(host string, cfg ConnConfig) (*ConnPool, error) {
	r := PoolRefiller{
		cfg: cfg,
	}
	if err := r.init(host); err != nil {
		return nil, err
	}

	go r.loop()

	return &r.pool, nil
}

func (p *ConnPool) String() string {
	return fmt.Sprintf("pool %s [shards=%d]", p.host, p.nrShards)
}

func (p *ConnPool) Conn(token Token) *Conn {
	idx := p.shardOf(token)
	if conn := p.loadConn(idx); conn != nil {
		if isHeavyLoaded(conn) {
			return p.maybeReplaceWithLessBusyConn(conn)
		}
		return conn
	}
	return p.LeastBusyConn()
}

func isHeavyLoaded(conn *Conn) bool {
	return conn.Waiting() > maxStreamID>>1
}

func (p *ConnPool) maybeReplaceWithLessBusyConn(conn *Conn) *Conn {
	if lb := p.LeastBusyConn(); conn.Waiting()-lb.Waiting() > maxStreamID<<1/10 {
		if p.connObs != nil {
			p.connObs.OnPickReplacedWithLessBusyConn(conn.Event())
		}
		return lb
	}
	return conn
}

func (p *ConnPool) LeastBusyConn() *Conn {
	var (
		leastBusyConn *Conn
		minBusy       = maxStreamID + 2 // adding 2 more is required due to atomics
	)

	for i := range p.conns {
		if conn := p.loadConn(i); conn != nil {
			if waiting := conn.Waiting(); waiting < minBusy {
				leastBusyConn = conn
				minBusy = waiting
			}
		}
	}
	return leastBusyConn
}

func (p *ConnPool) shardOf(token Token) int {
	shards := uint64(p.nrShards)
	z := uint64(token+math.MinInt64) << p.msbIgnore
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
	p.connClosedCh <- poolCloseShard
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
	addr     string
	pool     ConnPool
	cfg      ConnConfig
	active   int
	authFail []bool
}

func (r *PoolRefiller) init(host string) error {
	if err := r.cfg.validate(); err != nil {
		return fmt.Errorf("config validate :%w", err)
	}

	span := startSpan()
	conn, err := OpenConn(host, nil, r.cfg)
	span.stop()
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return err
	}

	s, err := conn.Supported()
	if err != nil {
		conn.Close()
		return fmt.Errorf("supported: %w", err)
	}

	ss := s.ScyllaSupported()
	if r.cfg.TLSConfig != nil {
		if v, ok := s.Options[ScyllaShardAwarePortSSL]; ok {
			r.addr = net.JoinHostPort(host, v[0])
		} else {
			return fmt.Errorf("missing encrypted shard aware port information %v", s.Options)
		}
	} else {
		if v, ok := s.Options[ScyllaShardAwarePort]; ok {
			r.addr = net.JoinHostPort(host, v[0])
		} else {
			return fmt.Errorf("missing shard aware port information %v", s.Options)
		}
	}

	r.pool = ConnPool{
		host:         host,
		nrShards:     int(ss.NrShards),
		msbIgnore:    ss.MsbIgnore,
		conns:        make([]atomic.Value, int(ss.NrShards)),
		connClosedCh: make(chan int, int(ss.NrShards)+1),
		connObs:      r.cfg.ConnObserver,
	}

	conn.setOnClose(r.onConnClose)
	r.pool.storeConn(conn)
	r.active = 1
	r.authFail = make([]bool, int(ss.NrShards))
	if r.pool.connObs != nil {
		r.pool.connObs.OnConnect(ConnectEvent{ConnEvent: conn.Event(), span: span})
	}

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
			if shard == poolCloseShard {
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
		if r.authFail[i] || r.pool.loadConn(i) != nil {
			continue
		}

		si.Shard = uint16(i)
		span := startSpan()
		conn, err := OpenShardConn(r.addr, si, r.cfg)
		span.stop()
		if err != nil {
			e, ok := err.(CodedError)
			if ok && e.ErrorCode() == frame.ErrCodeCredentials {
				r.authFail[i] = true
			}
			if r.pool.connObs != nil {
				r.pool.connObs.OnConnect(ConnectEvent{ConnEvent: ConnEvent{Addr: r.addr, Shard: si.Shard}, span: span, Err: err})
			}
			if conn != nil {
				conn.Close()
			}
			continue
		}
		if r.pool.connObs != nil {
			r.pool.connObs.OnConnect(ConnectEvent{ConnEvent: conn.Event(), span: span})
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
