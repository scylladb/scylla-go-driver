package response

import (
	"github.com/google/go-cmp/cmp"
	"scylla-go-driver/frame"
	"testing"
)

func TestStatusChangeEvent(t *testing.T) {
	cases := []struct {
		name     string
		content  []byte
		expected StatusChange
	}{
		{"UP",
			frame.MassAppendBytes(frame.StringToBytes("UP"),
				frame.InetToBytes(frame.Inet{
					IP:   []byte{127, 0, 0, 1},
					Port: 9042,
				})),
			StatusChange{
				Status: "UP",
				Address: frame.Inet{
					IP:   []byte{127, 0, 0, 1},
					Port: 9042,
				}}},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			var buf frame.Buffer
			buf.Write(v.content)
			a := ParseStatusChange(&buf)
			if diff := cmp.Diff(a, v.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestTopologyChangeEvent(t *testing.T) {
	cases := []struct {
		name     string
		content  []byte
		expected TopologyChange
	}{
		{"NEW_NODE",
			frame.MassAppendBytes(frame.StringToBytes("NEW_NODE"),
				frame.InetToBytes(frame.Inet{
					IP:   []byte{127, 0, 0, 1},
					Port: 9042,
				})),
			TopologyChange{
				Change: "NEW_NODE",
				Address: frame.Inet{
					IP:   []byte{127, 0, 0, 1},
					Port: 9042,
				}}},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			var buf frame.Buffer
			buf.Write(v.content)
			a := ParseTopologyChange(&buf)
			if diff := cmp.Diff(a, v.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestSchemaChangeEvent(t *testing.T) {
	cases := []struct {
		name     string
		content  []byte
		expected SchemaChange
	}{
		{"KEYSPACE",
			frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("KEYSPACE"),
				frame.StringToBytes("test")),
			SchemaChange{Change: "CREATED", Target: "KEYSPACE", Keyspace: "test"}},
		{"TABLE",
			frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("TABLE"),
				frame.StringToBytes("test"),
				frame.StringToBytes("mytable")),
			SchemaChange{Change: "CREATED",
				Target:   "TABLE",
				Keyspace: "test",
				Object:   "mytable"}},
		{"TYPE",
			frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("TYPE"),
				frame.StringToBytes("test"),
				frame.StringToBytes("mytype")),
			SchemaChange{Change: "CREATED",
				Target:   "TYPE",
				Keyspace: "test",
				Object:   "mytype"}},
		{"FUNCTION",
			frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("FUNCTION"),
				frame.StringToBytes("test"),
				frame.StringToBytes("myfunction"),
				frame.StringListToBytes([]string{"int", "int"})),
			SchemaChange{Change: "CREATED",
				Target:    "FUNCTION",
				Keyspace:  "test",
				Object:    "myfunction",
				Arguments: []string{"int", "int"}}},
		{"AGGREGATE",
			frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("AGGREGATE"),
				frame.StringToBytes("test"),
				frame.StringToBytes("myaggregate"),
				frame.StringListToBytes([]string{"int", "int"})),
			SchemaChange{Change: "CREATED",
				Target:    "AGGREGATE",
				Keyspace:  "test",
				Object:    "myaggregate",
				Arguments: []string{"int", "int"}}},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			var buf frame.Buffer
			buf.Write(v.content)
			s := ParseSchemaChange(&buf)
			if diff := cmp.Diff(s, v.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
