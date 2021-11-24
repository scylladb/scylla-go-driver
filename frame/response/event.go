package response

import (
	"fmt"
	"scylla-go-driver/frame"
)

type TopologyChange struct {
	Change  frame.TopologyChangeType
	Address frame.Inet
}

func ParseTopologyChange(b *frame.Buffer) (TopologyChange, error) {
	return TopologyChange{
		Change:  b.ParseTopologyChangeType(),
		Address: b.ReadInet(),
	}, b.Error()
}

type StatusChange struct {
	Status  frame.StatusChangeType
	Address frame.Inet
}

func ParseStatusChange(b *frame.Buffer) (StatusChange, error) {
	return StatusChange{
		Status:  b.ParseStatusChangeType(),
		Address: b.ReadInet(),
	}, b.Error()
}

type SchemaChange struct {
	Change    frame.SchemaChangeType
	Target    frame.SchemaChangeTarget
	Keyspace  string
	Object    string
	Arguments frame.StringList
}

func ParseSchemaChange(b *frame.Buffer) (SchemaChange, error) {
	c := b.ParseSchemaChangeType()
	t := b.ParseSchemaChangeTarget()
	switch t {
	case frame.Keyspace:
		return SchemaChange{
			Change:   c,
			Target:   t,
			Keyspace: b.ReadString(),
		}, b.Error()
	case frame.Table, frame.UserType:
		return SchemaChange{
			Change:   c,
			Target:   t,
			Keyspace: b.ReadString(),
			Object:   b.ReadString(),
		}, b.Error()
	case frame.Function, frame.Aggregate:
		return SchemaChange{
			Change:    c,
			Target:    t,
			Keyspace:  b.ReadString(),
			Object:    b.ReadString(),
			Arguments: b.ReadStringList(),
		}, b.Error()
	default:
		b.RecordError(fmt.Errorf("invalid SchemaChangeTarget: %s", t))
	}
	return SchemaChange{}, b.Error()
}
