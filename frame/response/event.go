package response

import (
	"bytes"
	"errors"
	"scylla-go-driver/frame"
)

var (
	unknownChangeType = errors.New("unknown type of change inside the event")
	unknownTarget     = errors.New("unknown target inside Schema Change event")
)

// TopologyChange response event type.
type TopologyChange struct {
	Change  string
	Address frame.Inet
}

// Valid types of change inside TopologyChange event.
var (
	newNode     = "NEW_NODE"
	removedNode = "REMOVED_NODE"
)

// ReadTopologyChange reads and returns TopologyChange from the buffer.
func ReadTopologyChange(b *bytes.Buffer) TopologyChange {
	c := frame.ReadString(b)
	if c != newNode && c != removedNode {
		panic(unknownChangeType)
	}
	return TopologyChange{c, frame.ReadInet(b)}
}

// StatusChange response event type.
type StatusChange struct {
	Status  string
	Address frame.Inet
}

// Valid types of change inside StatusChange event.
var (
	up   = "UP"
	down = "DOWN"
)

// ReadStatusChange reads and returns StatusChange from the buffer.
func ReadStatusChange(b *bytes.Buffer) StatusChange {
	s := frame.ReadString(b)
	if s != up && s != down {
		panic(unknownChangeType)
	}
	return StatusChange{s, frame.ReadInet(b)}
}

// SchemaChange response event type.
// Consists of attributes required for all types of target.
// Unnecessary attributes for a given target are uninitialized.
type SchemaChange struct {
	Change    string
	Target    string
	Keyspace  string
	Object    string
	Arguments frame.StringList
}

var (
	// Valid types of change inside SchemaChange event.
	created = "CREATED"
	updated = "UPDATED"
	dropped = "DROPPED"

	// Valid types of target inside SchemaChange event.
	keyspace  = "KEYSPACE"
	table     = "TABLE"
	userType  = "TYPE"
	function  = "FUNCTION"
	aggregate = "AGGREGATE"
)

// ReadSchemaChange reads and return SchemaChange from the buffer.
func ReadSchemaChange(b *bytes.Buffer) SchemaChange {
	c := frame.ReadString(b)
	if c != created && c != updated && c != dropped {
		panic(unknownChangeType)
	}
	t := frame.ReadString(b)
	switch t {
	case keyspace:
		return SchemaChange{
			Change:   c,
			Target:   t,
			Keyspace: frame.ReadString(b),
		}
	case table, userType:
		return SchemaChange{
			Change:   c,
			Target:   t,
			Keyspace: frame.ReadString(b),
			Object:   frame.ReadString(b),
		}
	case function, aggregate:
		return SchemaChange{
			Change:    c,
			Target:    t,
			Keyspace:  frame.ReadString(b),
			Object:    frame.ReadString(b),
			Arguments: frame.ReadStringList(b),
		}
	default:
		panic(unknownTarget)
	}
}
