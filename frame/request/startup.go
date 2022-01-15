package request

import (
	"scylla-go-driver/frame"
)

// Startup spec https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L285.
type Startup struct {
	Options frame.StartupOptions
}

func (s *Startup) WriteTo(b *frame.Buffer) {
	b.WriteStartupOptions(s.Options)
}

func (*Startup) OpCode() frame.OpCode {
	return frame.OpStartup
}
