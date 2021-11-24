package response

import (
	"fmt"
	"scylla-go-driver/frame"
)

type TopologyChange struct {
	Change  frame.TopologyChangeType
	Address frame.Inet
}

// Is returning error directly more elegant than returning it by frame.Buffer?
func GetTopologyChange(b frame.Buffer) (TopologyChange, error) {
	return TopologyChange{
		Change:  b.GetTopologyChangeType(),
		Address: b.GetInet(),
	}, b.Error
}

type StatusChange struct {
	Status  frame.StatusChangeType
	Address frame.Inet
}

func ReadStatusChange(b frame.Buffer) (StatusChange, error) {
	return StatusChange{
		Status:  b.GetStatusChangeType(),
		Address: b.GetInet(),
	}, b.Error
}

type SchemaChange struct {
	Change    frame.SchemaChangeType
	Target    frame.SchemaChangeTarget
	Keyspace  string
	Object    string
	Arguments frame.StringList
}

func ReadSchemaChange(b frame.Buffer) (SchemaChange, error) {
	c := b.GetSchemaChangeType()
	t := b.GetSchemaChangeTarget()
	switch t {
	case frame.Keyspace:
		return SchemaChange{
			Change:   c,
			Target:   t,
			Keyspace: b.GetString(),
		}, b.Error
	case frame.Table, frame.UserType:
		return SchemaChange{
			Change:   c,
			Target:   t,
			Keyspace: b.GetString(),
			Object:   b.GetString(),
		}, b.Error
	case frame.Function, frame.Aggregate:
		return SchemaChange{
			Change:    c,
			Target:    t,
			Keyspace:  b.GetString(),
			Object:    b.GetString(),
			Arguments: b.GetStringList(),
		}, b.Error
	default:
		b.RecordError(fmt.Errorf("invalid SchemaChangeTarget: %s", t))
	}
	return SchemaChange{}, b.Error
}
