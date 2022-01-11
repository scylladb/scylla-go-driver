package response

import (
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestStatusChangeEvent(t *testing.T) { // nolint:dupl // Tests are different.
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected StatusChange
	}{
		{
			"UP",
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
				},
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			a := ParseStatusChange(&buf)
			if diff := cmp.Diff(a, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestTopologyChangeEvent(t *testing.T) { //nolint:dupl // Tests are different.
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected TopologyChange
	}{
		{
			"NEW_NODE",
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
				},
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected SchemaChange
	}{
		{
			"KEYSPACE",
			frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("KEYSPACE"),
				frame.StringToBytes("test")),
			SchemaChange{Change: "CREATED", Target: "KEYSPACE", Keyspace: "test"},
		},
		{
			"TABLE",
			frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("TABLE"),
				frame.StringToBytes("test"),
				frame.StringToBytes("mytable")),
			SchemaChange{
				Change:   "CREATED",
				Target:   "TABLE",
				Keyspace: "test",
				Object:   "mytable",
			},
		},
		{
			"TYPE",
			frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("TYPE"),
				frame.StringToBytes("test"),
				frame.StringToBytes("mytype")),
			SchemaChange{
				Change:   "CREATED",
				Target:   "TYPE",
				Keyspace: "test",
				Object:   "mytype",
			},
		},
		{
			"FUNCTION",
			frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("FUNCTION"),
				frame.StringToBytes("test"),
				frame.StringToBytes("myfunction"),
				frame.StringListToBytes([]string{"int", "int"})),
			SchemaChange{
				Change:    "CREATED",
				Target:    "FUNCTION",
				Keyspace:  "test",
				Object:    "myfunction",
				Arguments: []string{"int", "int"},
			},
		},
		{
			"AGGREGATE",
			frame.MassAppendBytes(frame.StringToBytes("CREATED"),
				frame.StringToBytes("AGGREGATE"),
				frame.StringToBytes("test"),
				frame.StringToBytes("myaggregate"),
				frame.StringListToBytes([]string{"int", "int"})),
			SchemaChange{
				Change:    "CREATED",
				Target:    "AGGREGATE",
				Keyspace:  "test",
				Object:    "myaggregate",
				Arguments: []string{"int", "int"},
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			s := ParseSchemaChange(&buf)
			if diff := cmp.Diff(s, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
