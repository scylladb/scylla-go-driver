package response

import (
	"scylla-go-driver/frame"
)

type Authenticate struct {
	Name string
}

func ParseAuthenticate(b *frame.Buffer) (Authenticate, error) {
	return Authenticate{
		Name: b.ReadString(),
	}, b.Error()
}
