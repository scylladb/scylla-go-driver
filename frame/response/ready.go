package response

import (
	"bytes"
)

type Ready struct {
}

func ReadReady(_ *bytes.Buffer) Ready {
	return Ready{}
}
