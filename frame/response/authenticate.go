package response

import (
	"bytes"
	"scylla-go-driver/frame"
)

type Authenticate struct {
	Name string
}

func ReadAuthenticate(b *bytes.Buffer) Authenticate {
	return Authenticate{Name: frame.ReadString(b)}
}
