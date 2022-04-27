package transport

import (
	"fmt"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

func responseAsError(res frame.Response) error {
	if v, ok := res.(error); ok {
		return v
	}
	return fmt.Errorf("unexpected response %T, %+v", res, res)
}
