package response

import (
	"scylla-go-driver/frame"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func ErrToBytes(err Error) []byte {
	var out frame.Buffer
	out.WriteInt(err.Code)
	out.WriteString(err.Message)
	return out.Bytes()
}

func TestValidErrorCodes(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected Error
	}{
		{
			name:     "server",
			content:  ErrToBytes(Error{0x0000, "message 1"}),
			expected: Error{frame.ErrCodeServer, "message 1"},
		},
		{
			name:     "protocol",
			content:  ErrToBytes(Error{0x000a, "message 1"}),
			expected: Error{frame.ErrCodeProtocol, "message 1"},
		},
		{
			name:     "authentication",
			content:  ErrToBytes(Error{0x0100, "message 1"}),
			expected: Error{frame.ErrCodeCredentials, "message 1"},
		},
		{
			name:     "overload",
			content:  ErrToBytes(Error{0x1001, "message 1"}),
			expected: Error{frame.ErrCodeOverloaded, "message 1"},
		},
		{
			name:     "is_bootstrapping",
			content:  ErrToBytes(Error{0x1002, "message 1"}),
			expected: Error{frame.ErrCodeBootstrapping, "message 1"},
		},
		{
			name:     "truncate",
			content:  ErrToBytes(Error{0x1003, "message 1"}),
			expected: Error{frame.ErrCodeTruncate, "message 1"},
		},
		{
			name:     "syntax",
			content:  ErrToBytes(Error{0x2000, "message 1"}),
			expected: Error{frame.ErrCodeSyntax, "message 1"},
		},
		{
			name:     "unauthorized",
			content:  ErrToBytes(Error{0x2100, "message 1"}),
			expected: Error{frame.ErrCodeUnauthorized, "message 1"},
		},
		{
			name:     "invalid",
			content:  ErrToBytes(Error{0x2200, "message 1"}),
			expected: Error{frame.ErrCodeInvalid, "message 1"},
		},
		{
			name:     "config",
			content:  ErrToBytes(Error{0x2300, "message 1"}),
			expected: Error{frame.ErrCodeConfig, "message 1"},
		},
	}

	var buf frame.Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out := ParseError(&buf)
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing base error type.")
			}
		})
		buf.Reset()
	}
}

func TestUnavailableError(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected UnavailableError
	}{
		{
			name: "unavailable",
			content: frame.MassAppendBytes(ErrToBytes(Error{0x1000, "message 2"}),
				frame.ShortToBytes(frame.Consistency(1)),
				frame.IntToBytes(frame.Int(2)),
				frame.IntToBytes(frame.Int(3))),
			expected: UnavailableError{
				Error{0x1000, "message 2"}, 1, 2, 3,
			},
		},
	}

	var buf frame.Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out := ParseUnavailableError(&buf)
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing 'Unavailable' error.")
			}
		})

		buf.Reset()
	}
}

func TestWriteTimeoutError(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected WriteTimeoutError
	}{
		{
			name: "write timeout",
			content: frame.MassAppendBytes(ErrToBytes(Error{0x1100, "message 2"}),
				frame.ShortToBytes(frame.Short(0x0004)),
				frame.IntToBytes(frame.Int(-5)),
				frame.IntToBytes(frame.Int(100)),
				frame.StringToBytes("SIMPLE")),
			expected: WriteTimeoutError{
				Error{0x1100, "message 2"}, 0x0004, -5, 100, "SIMPLE",
			},
		},
	}

	var buf frame.Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out := ParseWriteTimeoutError(&buf)
			if out != tc.expected {
				t.Fatal("Failure while constructing 'WriteTo Timeout' error.")
			}
		})
		buf.Reset()
	}
}

func TestReadTimeoutError(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected ReadTimeoutError
	}{
		{
			name: "write timeout",
			content: frame.MassAppendBytes(ErrToBytes(Error{0x1200, "message 2"}),
				frame.ShortToBytes(frame.Short(0x0002)),
				frame.IntToBytes(frame.Int(8)),
				frame.IntToBytes(frame.Int(32)),
				frame.ByteToBytes(0)),
			expected: ReadTimeoutError{
				Error{0x1200, "message 2"}, 0x0002, 8, 32, 0,
			},
		},
	}

	var buf frame.Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out := ParseReadTimeoutError(&buf)
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing 'WriteTo Timeout' error.")
			}
		})
		buf.Reset()
	}
}

func TestReadFailureError(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected ReadFailureError
	}{
		{
			name: "write timeout",
			content: frame.MassAppendBytes(ErrToBytes(Error{0x1300, "message 2"}),
				frame.ShortToBytes(frame.Short(0x0003)),
				frame.IntToBytes(frame.Int(4)),
				frame.IntToBytes(frame.Int(5)),
				frame.IntToBytes(frame.Int(6)),
				frame.ByteToBytes(123)),
			expected: ReadFailureError{
				Error{0x1300, "message 2"}, 0x0003, 4, 5, 6, 123,
			},
		},
	}

	var buf frame.Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out := ParseReadFailureError(&buf)
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing 'WriteTo Timeout' error.")
			}
		})
		buf.Reset()
	}
}

func TestFuncFailureError(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected FuncFailureError
	}{
		{
			name: "write timeout",
			content: frame.MassAppendBytes(ErrToBytes(Error{0x1400, "message 2"}),
				frame.StringToBytes("keyspace_name"),
				frame.StringToBytes("function_name"),
				frame.StringListToBytes([]string{"type1", "type2"})),
			expected: FuncFailureError{
				Error{0x1400, "message 2"}, "keyspace_name", "function_name", []string{"type1", "type2"},
			},
		},
	}

	var buf frame.Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out := ParseFuncFailureError(&buf)
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing 'Function Failure' error.")
			}
		})
		buf.Reset()
	}
}

func TestWriteFailureError(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected WriteFailureError
	}{
		{
			name: "write timeout",
			content: frame.MassAppendBytes(ErrToBytes(Error{0x1500, "message 2"}),
				frame.ShortToBytes(0x0000),
				frame.IntToBytes(2),
				frame.IntToBytes(4),
				frame.IntToBytes(8),
				frame.StringToBytes("COUNTER")),
			expected: WriteFailureError{
				Error{0x1500, "message 2"}, 0x0000, 2, 4, 8, "COUNTER",
			},
		},
	}

	var buf frame.Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out := ParseWriteFailureError(&buf)
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing 'Function Failure' error.")
			}
		})
		buf.Reset()
	}
}

func TestAlreadyExistsError(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected AlreadyExistsError
	}{
		{
			name: "write timeout",
			content: frame.MassAppendBytes(ErrToBytes(Error{0x2400, "message 2"}),
				frame.StringToBytes("keyspace_name"),
				frame.StringToBytes("table_name")),
			expected: AlreadyExistsError{
				Error{0x2400, "message 2"}, "keyspace_name", "table_name",
			},
		},
	}
	t.Parallel()
	var buf frame.Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out := ParseAlreadyExistsError(&buf)
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
		buf.Reset()
	}
}

// There are no tests for unprepared error in rust nor java
func TestUnpreparedError(t *testing.T) {
	var testCases = []struct {
		name     string
		content  []byte
		expected UnpreparedError
	}{
		{
			name: "write timeout",
			content: frame.MassAppendBytes(ErrToBytes(Error{0x2500, "message 2"}),
				frame.BytesToShortBytes([]byte{1, 2, 3})),
			expected: UnpreparedError{
				Error{0x2500, "message 2"}, []byte{1, 2, 3},
			},
		},
	}
	t.Parallel()
	var buf frame.Buffer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out := ParseUnpreparedError(&buf)
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
		buf.Reset()
	}
}
