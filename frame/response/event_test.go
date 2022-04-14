package response

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"

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
			name: "UP",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString("UP")
				b.WriteInet(frame.Inet{
					IP:   []byte{127, 0, 0, 1},
					Port: 9042,
				})
				return b.Bytes()
			}(),
			expected: StatusChange{
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
			if diff := cmp.Diff(*a, tc.expected); diff != "" {
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
			name: "NEW_NODE",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString("NEW_NODE")
				b.WriteInet(frame.Inet{
					IP:   []byte{127, 0, 0, 1},
					Port: 9042,
				})
				return b.Bytes()
			}(),
			expected: TopologyChange{
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
			if diff := cmp.Diff(*a, tc.expected); diff != "" {
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
			name: "KEYSPACE",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString("CREATED")
				b.WriteString("KEYSPACE")
				b.WriteString("test")
				return b.Bytes()
			}(),
			expected: SchemaChange{Change: "CREATED", Target: "KEYSPACE", Keyspace: "test"},
		},
		{
			name: "TABLE",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString("CREATED")
				b.WriteString("TABLE")
				b.WriteString("test")
				b.WriteString("mytable")
				return b.Bytes()
			}(),
			expected: SchemaChange{
				Change:   "CREATED",
				Target:   "TABLE",
				Keyspace: "test",
				Object:   "mytable",
			},
		},
		{
			name: "TYPE",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString("CREATED")
				b.WriteString("TYPE")
				b.WriteString("test")
				b.WriteString("mytype")
				return b.Bytes()
			}(),
			expected: SchemaChange{
				Change:   "CREATED",
				Target:   "TYPE",
				Keyspace: "test",
				Object:   "mytype",
			},
		},
		{
			name: "FUNCTION",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString("CREATED")
				b.WriteString("FUNCTION")
				b.WriteString("test")
				b.WriteString("myfunction")
				b.WriteStringList([]string{"int", "int"})
				return b.Bytes()
			}(),
			expected: SchemaChange{
				Change:    "CREATED",
				Target:    "FUNCTION",
				Keyspace:  "test",
				Object:    "myfunction",
				Arguments: []string{"int", "int"},
			},
		},
		{
			name: "AGGREGATE",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString("CREATED")
				b.WriteString("AGGREGATE")
				b.WriteString("test")
				b.WriteString("myaggregate")
				b.WriteStringList([]string{"int", "int"})
				return b.Bytes()
			}(),
			expected: SchemaChange{
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
			if diff := cmp.Diff(*s, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

var (
	dummyStaC *StatusChange
	dummyTopC *TopologyChange
	dummySchC *SchemaChange
)

// We want to make sure that parsing does not crush driver even for random data.
// We assign result to global variable to avoid compiler optimization.
func FuzzStatusChange(f *testing.F) {
	testCases := [][]byte{make([]byte, 1000000)}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseStatusChange(&buf)
		dummyStaC = out
	})
}

func FuzzTopologyChange(f *testing.F) {
	testCases := [][]byte{make([]byte, 1000000)}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseTopologyChange(&buf)
		dummyTopC = out
	})
}

func FuzzSchemaChange(f *testing.F) {
	testCases := [][]byte{make([]byte, 1000000)}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseSchemaChange(&buf)
		dummySchC = out
	})
}
