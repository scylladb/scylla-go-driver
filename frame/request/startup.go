package request

import (
	"scylla-go-driver/frame"
)

type Startup struct {
	Options frame.StartupOptions
}

func (s Startup) WriteTo(b *frame.Buffer) {
	b.WriteStartupOptions(s.Options)
}
