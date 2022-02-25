package transport

import (
	"fmt"

	"scylla-go-driver/frame"
	. "scylla-go-driver/frame/response"
)

func responseAsError(res frame.Response) error {
	if v, ok := res.(*Error); ok {
		return v
	}
	return fmt.Errorf("unexpected response %T, %+v", res, res)
}
