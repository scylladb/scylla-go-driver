package response

import (
	"scylla-go-driver/frame"
)

type AuthSuccess struct {
	Token frame.Bytes
}

func ParseAuthSuccess(b *frame.Buffer) (AuthSuccess, error) {
	return AuthSuccess{
		Token: b.ReadBytes(),
	}, b.Error()
}
