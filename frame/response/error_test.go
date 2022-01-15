package response

import (
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func errToBytes(err Error) []byte {
	var out frame.Buffer
	out.WriteInt(err.Code)
	out.WriteString(err.Message)
	return out.Bytes()
}

func writeErrorTo(b *frame.Buffer, err Error) {
	for _, v := range errToBytes(err) {
		b.WriteByte(v)
	}
}

func TestValidErrorCodes(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected Error
	}{
		{
			name:     "server",
			content:  errToBytes(Error{0x0000, "message 1"}),
			expected: Error{frame.ErrCodeServer, "message 1"},
		},
		{
			name:     "protocol",
			content:  errToBytes(Error{0x000a, "message 1"}),
			expected: Error{frame.ErrCodeProtocol, "message 1"},
		},
		{
			name:     "authentication",
			content:  errToBytes(Error{0x0100, "message 1"}),
			expected: Error{frame.ErrCodeCredentials, "message 1"},
		},
		{
			name:     "overload",
			content:  errToBytes(Error{0x1001, "message 1"}),
			expected: Error{frame.ErrCodeOverloaded, "message 1"},
		},
		{
			name:     "is_bootstrapping",
			content:  errToBytes(Error{0x1002, "message 1"}),
			expected: Error{frame.ErrCodeBootstrapping, "message 1"},
		},
		{
			name:     "truncate",
			content:  errToBytes(Error{0x1003, "message 1"}),
			expected: Error{frame.ErrCodeTruncate, "message 1"},
		},
		{
			name:     "syntax",
			content:  errToBytes(Error{0x2000, "message 1"}),
			expected: Error{frame.ErrCodeSyntax, "message 1"},
		},
		{
			name:     "unauthorized",
			content:  errToBytes(Error{0x2100, "message 1"}),
			expected: Error{frame.ErrCodeUnauthorized, "message 1"},
		},
		{
			name:     "invalid",
			content:  errToBytes(Error{0x2200, "message 1"}),
			expected: Error{frame.ErrCodeInvalid, "message 1"},
		},
		{
			name:     "config",
			content:  errToBytes(Error{0x2300, "message 1"}),
			expected: Error{frame.ErrCodeConfig, "message 1"},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseError(&buf)
			if diff := cmp.Diff(*out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing base error type.")
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
				writeErrorTo(&b, Error{0x1000, "message 2"})
				b.WriteShort(frame.Consistency(1))
				b.WriteInt(frame.Int(2))
				b.WriteInt(frame.Int(3))
				return b.Bytes()
			}(),
			expected: UnavailableError{
				Error{0x1000, "message 2"}, 1, 2, 3,
			},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseUnavailableError(&buf)
			if diff := cmp.Diff(*out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing 'Unavailable' error.")
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
				writeErrorTo(&b, Error{0x1100, "message 2"})
				b.WriteShort(frame.Short(0x0004))
				b.WriteInt(frame.Int(-5))
				b.WriteInt(frame.Int(100))
				b.WriteString("SIMPLE")
				return b.Bytes()
			}(),
			expected: WriteTimeoutError{
				Error{0x1100, "message 2"}, 0x0004, -5, 100, "SIMPLE",
			},
		},
	}

	var buf frame.Buffer
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			buf.Write(tc.content)
			out := ParseWriteTimeoutError(&buf)
			if diff := cmp.Diff(*out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing 'WriteTo Timeout' error.")
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
			name: "write timeout",
			content: func() []byte {
				var b frame.Buffer
				writeErrorTo(&b, Error{0x1200, "message 2"})
				b.WriteShort(frame.Short(0x0002))
				b.WriteInt(frame.Int(8))
				b.WriteInt(frame.Int(32))
				b.WriteByte(0)
				return b.Bytes()
			}(),
			expected: ReadTimeoutError{
				Error{0x1200, "message 2"}, 0x0002, 8, 32, 0,
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseReadTimeoutError(&buf)
			if diff := cmp.Diff(*out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing 'WriteTo Timeout' error.")
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
			name: "write timeout",
			content: func() []byte {
				var b frame.Buffer
				writeErrorTo(&b, Error{0x1300, "message 2"})
				b.WriteShort(frame.Short(0x0003))
				b.WriteInt(frame.Int(4))
				b.WriteInt(frame.Int(5))
				b.WriteInt(frame.Int(6))
				b.WriteByte(123)
				return b.Bytes()
			}(),
			expected: ReadFailureError{
				Error{0x1300, "message 2"}, 0x0003, 4, 5, 6, 123,
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseReadFailureError(&buf)
			if diff := cmp.Diff(*out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing 'WriteTo Timeout' error.")
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
			name: "write timeout",
			content: func() []byte {
				var b frame.Buffer
				writeErrorTo(&b, Error{0x1400, "message 2"})
				b.WriteString("keyspace_name")
				b.WriteString("function_name")
				b.WriteStringList([]string{"type1", "type2"})
				return b.Bytes()
			}(),
			expected: FuncFailureError{
				Error{0x1400, "message 2"}, "keyspace_name", "function_name", []string{"type1", "type2"},
			},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseFuncFailureError(&buf)
			if diff := cmp.Diff(*out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing 'Function Failure' error.")
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
			name: "write timeout",
			content: func() []byte {
				var b frame.Buffer
				writeErrorTo(&b, Error{0x1500, "message 2"})
				b.WriteShort(frame.Short(0x0000))
				b.WriteInt(frame.Int(2))
				b.WriteInt(frame.Int(4))
				b.WriteInt(frame.Int(8))
				b.WriteString("COUNTER")
				return b.Bytes()
			}(),
			expected: WriteFailureError{
				Error{0x1500, "message 2"}, 0x0000, 2, 4, 8, "COUNTER",
			},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseWriteFailureError(&buf)
			if diff := cmp.Diff(*out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing 'Function Failure' error.")
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
			name: "write timeout",
			content: func() []byte {
				var b frame.Buffer
				writeErrorTo(&b, Error{0x2400, "message 2"})
				b.WriteString("keyspace_name")
				b.WriteString("table_name")
				return b.Bytes()
			}(),
			expected: AlreadyExistsError{
				Error{0x2400, "message 2"}, "keyspace_name", "table_name",
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseAlreadyExistsError(&buf)
			if diff := cmp.Diff(*out, tc.expected); diff != "" {
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
			name: "write timeout",
			content: func() []byte {
				var b frame.Buffer
				writeErrorTo(&b, Error{0x2500, "message 2"})
				b.WriteShortBytes([]byte{1, 2, 3})
				return b.Bytes()
			}(),
			expected: UnpreparedError{
				Error{0x2500, "message 2"}, []byte{1, 2, 3},
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseUnpreparedError(&buf)
			if diff := cmp.Diff(*out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
