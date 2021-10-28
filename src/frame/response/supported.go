package response

import "scylla-go-driver/src/frame"

type Supported struct {
	frame.Header
	options frame.StringMultiMap
}
