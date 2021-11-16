package response

import (
	"bytes"
	"scylla-go-driver/frame"
)

type Authenticate struct {
	name string
}

func ReadAuthenticate(b *bytes.Buffer) Authenticate {
	return Authenticate{name: frame.ReadString(b)}
}
