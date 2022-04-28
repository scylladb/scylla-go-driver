package transport

import (
	"fmt"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

type IoError struct {
	message string
}

func (io *IoError) Error() string {
	return fmt.Sprintf("IoError: %s", io.message)
}

// responseAsError returns either IoError or some error defined in response.error.
func responseAsError(res frame.Response) error {
	if v, ok := res.(error); ok {
		return v
	}
	return &IoError{message: fmt.Sprintf("unexpected response %T, %+v", res, res)}
}
