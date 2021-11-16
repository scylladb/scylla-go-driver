package response

import "scylla-go-driver/frame"

type AuthSuccess struct {
	bytes frame.Bytes
}
