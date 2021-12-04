package response

import (
	"bytes"
)

// Ready spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L507
type Ready struct{}

func ParseReady(_ *bytes.Buffer) (Ready, error) {
	return Ready{}, nil
}
