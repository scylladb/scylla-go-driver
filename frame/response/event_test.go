package response

import (
	"scylla-go-driver/frame"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestStatusChangeEvent(t *testing.T) {
	testCases := []struct {
		name     string
		content  []byte
		expected StatusChange
	}{
		{
			name: "UP",
			content: frame.MassAppendBytes(frame.StringToBytes("UP"),
				frame.InetToBytes(frame.Inet{
					IP:   []byte{127, 0, 0, 1},
					Port: 9042,
				})),
			expected: StatusChange{
				Status: "UP",
				Address: frame.Inet{
					IP:   []byte{127, 0, 0, 1},
					Port: 9042,
				}}},
	}
	t.Parallel()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf frame.Buffer
			buf.Write(tc.content)
			a := ParseStatusChange(&buf)
			if diff := cmp.Diff(a, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestTopologyChangeEvent(t *testing.T) {
	testCases := []struct {
		name     string
		content  []byte
		expected TopologyChange
	}{
		{
			name: "NEW_NODE",
			content: frame.MassAppendBytes(frame.StringToBytes("NEW_NODE"),
				frame.InetToBytes(frame.Inet{
					IP:   []byte{127, 0, 0, 1},
					Port: 9042,
				})),
			expected: TopologyChange{
				Change: "NEW_NODE",
				Address: frame.Inet{
					IP:   []byte{127, 0, 0, 1},
					Port: 9042,
				}}},
	}
	t.Parallel()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf frame.Buffer
			buf.Write(tc.content)
			a := ParseTopologyChange(&buf)
			if diff := cmp.Diff(a, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestSchemaChangeEvent(t *testing.T) {
	testCases := []struct {
		name     string
		content  []byte
		expected SchemaChange
	}{
		{
			name: "KEYSPACE",
			content: frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("KEYSPACE"),
				frame.StringToBytes("test")),
			expected: SchemaChange{Change: "CREATED", Target: "KEYSPACE", Keyspace: "test"}},
		{
			name: "TABLE",
			content: frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("TABLE"),
				frame.StringToBytes("test"),
				frame.StringToBytes("mytable")),
			expected: SchemaChange{Change: "CREATED",
				Target:   "TABLE",
				Keyspace: "test",
				Object:   "mytable"}},
		{
			name: "TYPE",
			content: frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("TYPE"),
				frame.StringToBytes("test"),
				frame.StringToBytes("mytype")),
			expected: SchemaChange{Change: "CREATED",
				Target:   "TYPE",
				Keyspace: "test",
				Object:   "mytype"}},
		{
			name: "FUNCTION",
			content: frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("FUNCTION"),
				frame.StringToBytes("test"),
				frame.StringToBytes("myfunction"),
				frame.StringListToBytes([]string{"int", "int"})),
			expected: SchemaChange{Change: "CREATED",
				Target:    "FUNCTION",
				Keyspace:  "test",
				Object:    "myfunction",
				Arguments: []string{"int", "int"}}},
		{
			name: "AGGREGATE",
			content: frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("AGGREGATE"),
				frame.StringToBytes("test"),
				frame.StringToBytes("myaggregate"),
				frame.StringListToBytes([]string{"int", "int"})),
			expected: SchemaChange{Change: "CREATED",
				Target:    "AGGREGATE",
				Keyspace:  "test",
				Object:    "myaggregate",
				Arguments: []string{"int", "int"}}},
	}
	t.Parallel()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf frame.Buffer
			buf.Write(tc.content)
			s := ParseSchemaChange(&buf)
			if diff := cmp.Diff(s, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
