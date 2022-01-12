package transport

import (
	"errors"
	"fmt"
	"net"
	"scylla-go-driver/frame"
	"scylla-go-driver/frame/request"
	"scylla-go-driver/frame/response"
	"strconv"
)

const (
	//defaultPort = 9042

	peersQuery     = "SELECT * FROM system.peers"
	shardAwarePort = "SCYLLA_SHARD_AWARE_PORT"
	shardCnt       = "SCYLLA_NR_SHARDS"
	shard          = "SCYLLA_SHARD"
)

// PeerInfo will probably be moved to separate file.
type PeerInfo struct {
	Peer net.IP
	// Not used yet.
	//dataCenter string
	//hostID frame.UUID
	//preferredIP net.IP
	//rack string
	//releaseVersion string
	//rpcAddress net.IP
	//schemaVersion frame.UUID
	//supportedFeatures string // We will most likely change that to map.
	//tokens []int64 // We will most likely change that.
}

type ControlConn struct {
	session *Session
	conn    *Connection
}

func NewControlConn(addr string, session *Session) ControlConn {
	if c, err := NewConnection(addr); err == nil {
		return ControlConn{
			conn:    c,
			session: session,
		}
	}
	return ControlConn{}
}

func (c *ControlConn) RegisterEvents() chan ConnResponse {
	res := make(chan ConnResponse)
	req := ConnRequest{
		Body: request.Register{EventTypes: []frame.EventType{
			frame.TopologyChange,
			frame.SchemaChange,
			frame.StatusChange}},
		Code:     frame.OpRegister,
		Response: res,
	}
	c.conn.newTask <- req
	return res
}

func (c *ControlConn) DiscoverTopology() (map[string]PeerInfo, error) {
	// Not sure about this Query mechanism.
	m := make(map[string]PeerInfo)
	res := <-c.conn.Query(peersQuery)
	buf := frame.NewBuffer(res.Body)

	switch res.Header.Opcode {
	case frame.OpError:
		err := response.ParseError(&buf)
		return m, errors.New("error " + strconv.Itoa(int(err.Code)) + ": " + err.Message)
	case frame.OpResult:
		if n := buf.ReadInt(); n == 2 { // Should have frame.RowsKind.
			rows := response.ParseRowsResult(&buf)

			for _, v := range rows.RowsContent {
				// Watch out for copying data.
				ip := net.IP(v[0]) // Indexes should probably be named.
				m[ip.String()] = PeerInfo{Peer: ip}
			}

			//fmt.Printf("columnscnt: %v\n", rows.Metadata.ColumnsCnt)
			//fmt.Printf("rowscnt: %v\n", rows.RowsCnt)
			//
			//for _, v := range rows.Metadata.Columns {
			//	fmt.Printf("name: %v\n", v.Name)
			//	fmt.Printf("typeid: %v\n", v.Type.ID)
			//}
			//
			//fmt.Println(rows.RowsContent[0][0])
			//b := frame.NewBuffer(rows.RowsContent[0][0])
			//i := b.ReadInet()
			//
			//fmt.Printf("IP: %v\n", i.IP)
			//fmt.Printf("PORT: %v\n", i.Port)
		}
		return m, nil
	default:
		return m, errors.New("invalid response frame opcode for Query request")
	}
}

func (c *ControlConn) InitHostPool(host string) ([]*Connection, error) {
	conn, err := NewConnection(host)
	if err != nil {
		return make([]*Connection, 0), err
	}

	supp := conn.SendOptions()
	var cnt, curr, remoteShard int
	shardAware := false

	if _, ok := supp.Options[shardAwarePort]; ok {
		// Problem with docker scylla ports.
		// host = host[:strings.IndexByte(host, ':')] + v[0]
		shardAware = true
	}
	// Are those mandatory options?
	if v, ok := supp.Options[shardCnt]; ok {
		cnt, _ = strconv.Atoi(v[0])
	}
	if v, ok := supp.Options[shard]; ok {
		curr, _ = strconv.Atoi(v[0])
	}

	pool := make([]*Connection, cnt)
	pool[curr] = conn
	for i := 0; i < cnt; i++ {
		remoteShard = (c.session.unusedPort + i) % cnt
		if shardAware && remoteShard == curr {
			continue
		}

		// TODO: closing connections.
		if conn, err = NewDialerConnection(host, c.session.unusedPort+i); err != nil {
			c.session.unusedPort += i
			return pool, err
		} else if pool[remoteShard] == nil {
			// Just to see that we are actually connecting to correct shards.
			fmt.Printf("i: %v rs: %v  shard: %v\n", i, remoteShard, conn.SendOptions().Options[shard])
			pool[remoteShard] = conn
		}
	}

	c.session.unusedPort += cnt
	return pool, nil
}
