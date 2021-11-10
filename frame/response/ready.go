package response

import (
	"bytes"
)

type Ready struct {
}

// TODO is argument in function a good practice to keep consistency?
func ReadReady(b *bytes.Buffer) Ready {
	return Ready{}
}
