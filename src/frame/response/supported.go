package response

import "scylla-go-driver/src/frame"

type Supported struct {
	frame.Header
	options frame.StringMultiMap
}

func NewSupported(buf []byte) (*Supported, error) {
	o := new(Supported)
	hd, err := frame.NewHeader(buf)
	if err != nil {
		return nil, err
	}
	o.Header = *hd

	err = frame.ReadStringMultiMap(buf, o.options)
	if err != nil {
		return o, err
	}

	return o, nil
}
