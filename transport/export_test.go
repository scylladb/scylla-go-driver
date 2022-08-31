package transport

func (p *ConnPool) AllConns() []*Conn {
	var conns = make([]*Conn, len(p.conns))
	for i := range conns {
		conns[i] = p.loadConn(i)
	}
	return conns
}
