package transport

func (p *ConnPool) AllConns() []*Conn {
	var conns = make([]*Conn, len(p.conns))
	for i, v := range p.conns {
		conns[i], _ = v.Load().(*Conn)
	}
	return conns
}
