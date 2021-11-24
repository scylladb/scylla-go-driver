package response

import (
	"bytes"
	"fmt"
	"scylla-go-driver/frame"
)

// TopologyChange response event type.
type TopologyChange struct {
	Change  frame.TopologyChangeType
	Address frame.Inet
}

// ReadTopologyChange reads and returns TopologyChange from the buffer.
func ReadTopologyChange(b *bytes.Buffer) (TopologyChange, error) {
	t := frame.ReadTopologyChangeType(b)
	if _, ok := frame.TopologyChangeTypes[t]; !ok {
		return TopologyChange{}, fmt.Errorf("invalid TopologyChangeType: %s", t)
	}
	return TopologyChange{
		Change:  t,
		Address: frame.ReadInet(b),
	}, nil
}

// StatusChange response event type.
type StatusChange struct {
	Status  frame.StatusChangeType
	Address frame.Inet
}

// ReadStatusChange reads and returns StatusChange from the buffer.
func ReadStatusChange(b *bytes.Buffer) (StatusChange, error) {
	t := frame.ReadStatusChangeType(b)
	if _, ok := frame.StatusChangeTypes[t]; !ok {
		return StatusChange{}, fmt.Errorf("invalid StatusChangeType: %s", t)
	}
	return StatusChange{
		Status:  t,
		Address: frame.ReadInet(b),
	}, nil
}

// SchemaChange response event type.
// Consists of attributes required for all types of target.
// Unnecessary attributes for a given target are uninitialized.
type SchemaChange struct {
	Change    frame.SchemaChangeType
	Target    frame.SchemaChangeTarget
	Keyspace  string
	Object    string
	Arguments frame.StringList
}

// ReadSchemaChange reads and return SchemaChange from the buffer.
func ReadSchemaChange(b *bytes.Buffer) (SchemaChange, error) {
	c := frame.ReadSchemaChangeType(b)
	if _, ok := frame.SchemaChangeTypes[c]; !ok {
		return SchemaChange{}, fmt.Errorf("invalid SchemaChangeType: %s", c)
	}
	t := frame.ReadSchemaChangeTarget(b)
	switch t {
	case frame.Keyspace:
		return SchemaChange{
			Change:   c,
			Target:   t,
			Keyspace: frame.ReadString(b),
		}, nil
	case frame.Table, frame.UserType:
		return SchemaChange{
			Change:   c,
			Target:   t,
			Keyspace: frame.ReadString(b),
			Object:   frame.ReadString(b),
		}, nil
	case frame.Function, frame.Aggregate:
		return SchemaChange{
			Change:    c,
			Target:    t,
			Keyspace:  frame.ReadString(b),
			Object:    frame.ReadString(b),
			Arguments: frame.ReadStringList(b),
		}, nil
	default:
		return SchemaChange{}, fmt.Errorf("invalid SchemaChangeTarget: %s", t)
	}
}
