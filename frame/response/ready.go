package response

import (
	"bytes"
)

type Ready struct{}

func ParseReady(_ *bytes.Buffer) (Ready, error) {
	return Ready{}, nil
}
