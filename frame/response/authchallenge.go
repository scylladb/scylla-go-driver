package response

import (
	"scylla-go-driver/frame"
)

type AuthChallenge struct {
	Token frame.Bytes
}

func ParseAuthChallenge(b *frame.Buffer) (AuthChallenge, error) {
	return AuthChallenge{
		Token: b.ReadBytes(),
	}, b.Error()
}
