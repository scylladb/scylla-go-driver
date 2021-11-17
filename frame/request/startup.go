package request

import (
	"bytes"
	"errors"
	"scylla-go-driver/frame"
)

// Startup response message type.
type Startup struct {
	options frame.StringMap
}

var mandatoryOptionNotIncluded = errors.New("mandatory option has not been included in startup body")
var possibleOptionWrongKey = errors.New("possible option has invalid key in startup body")

// Mandatory values and keys that can be given in Startup body
// value in the map means option name and key means its possible values
var mandatoryOptions = frame.StringMultiMap{
	"CQL_VERSION": {"3.0.0"},
}

// Mandatory values and keys that can be given in Startup body
// value in the map means option name and key means its possible values
var possibleOptions = frame.StringMultiMap{
	"COMPRESSION": {
		"lz4",
		"snappy",
	},
	"NO_COMPACT":        {},
	"THROW_ON_OVERLOAD": {},
}

// WriteStartup checks validity of given StringMap and
// if everything checks out then writes it into a buffer
func (s Startup) WriteTo(b *bytes.Buffer) {
	for k, v := range mandatoryOptions {
		if mv, ok := s.options[k]; !(ok && frame.Contains(v, mv)) {
			panic(mandatoryOptionNotIncluded)
		}
	}

	for k, v := range possibleOptions {
		if mv, ok := s.options[k]; ok && !frame.Contains(v, mv) {
			panic(possibleOptionWrongKey)
		}
	}

	frame.WriteStringMap(s.options, b)
}
