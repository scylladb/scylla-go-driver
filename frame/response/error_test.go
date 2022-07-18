package response

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func errToBytes(err ScyllaError) []byte {
	var out frame.Buffer
	out.WriteInt(err.Code)
	out.WriteString(err.Message)
	return out.Bytes()
}

func TestParseScyllaError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected ScyllaError
	}{
		{
			name:     "unavailable",
			content:  errToBytes(ScyllaError{0x1000, "message 2"}),
			expected: ScyllaError{0x1000, "message 2"},
		},
		{
			name:     "write timeout",
			content:  errToBytes(ScyllaError{0x1100, "message 2"}),
			expected: ScyllaError{0x1100, "message 2"},
		},
		{
			name:     "read timeout",
			content:  errToBytes(ScyllaError{0x1200, "message 2"}),
			expected: ScyllaError{0x1200, "message 2"},
		},
		{
			name:     "read failure",
			content:  errToBytes(ScyllaError{0x1300, "message 2"}),
			expected: ScyllaError{0x1300, "message 2"},
		},
		{
			name:     "func failure",
			content:  errToBytes(ScyllaError{0x1400, "message 2"}),
			expected: ScyllaError{0x1400, "message 2"},
		},
		{
			name:     "write failure",
			content:  errToBytes(ScyllaError{0x1500, "message 2"}),
			expected: ScyllaError{0x1500, "message 2"},
		},
		{
			name:     "already exists",
			content:  errToBytes(ScyllaError{0x2400, "message 2"}),
			expected: ScyllaError{0x2400, "message 2"},
		},
		{
			name:     "unprepared",
			content:  errToBytes(ScyllaError{0x2500, "message 2"}),
			expected: ScyllaError{0x2500, "message 2"},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseScyllaError(&buf)
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("failure while constructing ScyllaError")
			}
		})
	}
}

func TestUnavailableError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected UnavailableError
	}{
		{
			name: "unavailable",
			content: func() []byte {
				var b frame.Buffer
				b.WriteShort(frame.Consistency(1))
				b.WriteInt(frame.Int(2))
				b.WriteInt(frame.Int(3))
				return b.Bytes()
			}(),
			expected: UnavailableError{
				Consistency: 1,
				Required:    2,
				Alive:       3,
			},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseUnavailableError(&buf, ScyllaError{})
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("failure while constructing 'Unavailable' error")
			}
		})
	}
}

func TestWriteTimeoutError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected WriteTimeoutError
	}{
		{
			name: "write timeout",
			content: func() []byte {
				var b frame.Buffer
				b.WriteShort(frame.Short(0x0004))
				b.WriteInt(frame.Int(-5))
				b.WriteInt(frame.Int(100))
				b.WriteString("SIMPLE")
				return b.Bytes()
			}(),
			expected: WriteTimeoutError{
				Consistency: 0x0004,
				Received:    -5,
				BlockFor:    100,
				WriteType:   "SIMPLE",
			},
		},
	}

	var buf frame.Buffer
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			buf.Write(tc.content)
			out := ParseWriteTimeoutError(&buf, ScyllaError{})
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("failure while constructing 'WriteTo Timeout' error")
			}
		})
		buf.Reset()
	}
}

func TestReadTimeoutError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected ReadTimeoutError
	}{
		{
			name: "read timeout",
			content: func() []byte {
				var b frame.Buffer
				b.WriteShort(frame.Short(0x0002))
				b.WriteInt(frame.Int(8))
				b.WriteInt(frame.Int(32))
				b.WriteByte(0)
				return b.Bytes()
			}(),
			expected: ReadTimeoutError{
				Consistency: 0x0002,
				Received:    8,
				BlockFor:    32,
				DataPresent: false,
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseReadTimeoutError(&buf, ScyllaError{})
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("failure while constructing 'WriteTo Timeout' error")
			}
		})
	}
}

func TestReadFailureError(t *testing.T) { // nolint:dupl // Tests are different.
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected ReadFailureError
	}{
		{
			name: "read failure",
			content: func() []byte {
				var b frame.Buffer
				b.WriteShort(frame.Short(0x0003))
				b.WriteInt(frame.Int(4))
				b.WriteInt(frame.Int(5))
				b.WriteInt(frame.Int(6))
				b.WriteByte(123)
				return b.Bytes()
			}(),
			expected: ReadFailureError{
				Consistency: 0x0003,
				Received:    4,
				BlockFor:    5,
				NumFailures: 6,
				DataPresent: true,
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseReadFailureError(&buf, ScyllaError{})
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("failure while constructing 'WriteTo Timeout' error")
			}
		})
	}
}

func TestFuncFailureError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected FuncFailureError
	}{
		{
			name: "func failure",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString("keyspace_name")
				b.WriteString("function_name")
				b.WriteStringList([]string{"type1", "type2"})
				return b.Bytes()
			}(),
			expected: FuncFailureError{
				Keyspace: "keyspace_name",
				Function: "function_name",
				ArgTypes: []string{"type1", "type2"},
			},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseFuncFailureError(&buf, ScyllaError{})
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("failure while constructing 'Function Failure' error")
			}
		})
	}
}

func TestWriteFailureError(t *testing.T) { // nolint:dupl // Tests are different.
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected WriteFailureError
	}{
		{
			name: "write failure",
			content: func() []byte {
				var b frame.Buffer
				b.WriteShort(frame.Short(0x0000))
				b.WriteInt(frame.Int(2))
				b.WriteInt(frame.Int(4))
				b.WriteInt(frame.Int(8))
				b.WriteString("COUNTER")
				return b.Bytes()
			}(),
			expected: WriteFailureError{
				Consistency: 0x0000,
				Received:    2,
				BlockFor:    4,
				NumFailures: 8,
				WriteType:   "COUNTER",
			},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseWriteFailureError(&buf, ScyllaError{})
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("failure while constructing 'Function Failure' error")
			}
		})
	}
}

func TestAlreadyExistsError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected AlreadyExistsError
	}{
		{
			name: "already exists",
			content: func() []byte {
				var b frame.Buffer
				b.WriteString("keyspace_name")
				b.WriteString("table_name")
				return b.Bytes()
			}(),
			expected: AlreadyExistsError{
				Keyspace: "keyspace_name",
				Table:    "table_name",
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseAlreadyExistsError(&buf, ScyllaError{})
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestUnpreparedError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected UnpreparedError
	}{
		{
			name: "unprepared",
			content: func() []byte {
				var b frame.Buffer
				b.WriteShortBytes([]byte{1, 2, 3})
				return b.Bytes()
			}(),
			expected: UnpreparedError{
				UnknownID: []byte{1, 2, 3},
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseUnpreparedError(&buf, ScyllaError{})
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
