package response

import (
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func ErrToBytes(err Error) []byte {
	var out frame.Buffer
	out.WriteInt(err.Code)
	out.WriteString(err.Message)
	return out.Bytes()
}

func TestValidErrorCodes(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected Error
	}{
		{
			"server",
			ErrToBytes(Error{0x0000, "message 1"}),
			Error{frame.ErrCodeServer, "message 1"},
		},
		{
			"protocol",
			ErrToBytes(Error{0x000a, "message 1"}),
			Error{frame.ErrCodeProtocol, "message 1"},
		},
		{
			"authentication",
			ErrToBytes(Error{0x0100, "message 1"}),
			Error{frame.ErrCodeCredentials, "message 1"},
		},
		{
			"overload",
			ErrToBytes(Error{0x1001, "message 1"}),
			Error{frame.ErrCodeOverloaded, "message 1"},
		},
		{
			"is_bootstrapping",
			ErrToBytes(Error{0x1002, "message 1"}),
			Error{frame.ErrCodeBootstrapping, "message 1"},
		},
		{
			"truncate",
			ErrToBytes(Error{0x1003, "message 1"}),
			Error{frame.ErrCodeTruncate, "message 1"},
		},
		{
			"syntax",
			ErrToBytes(Error{0x2000, "message 1"}),
			Error{frame.ErrCodeSyntax, "message 1"},
		},
		{
			"unauthorized",
			ErrToBytes(Error{0x2100, "message 1"}),
			Error{frame.ErrCodeUnauthorized, "message 1"},
		},
		{
			"invalid",
			ErrToBytes(Error{0x2200, "message 1"}),
			Error{frame.ErrCodeInvalid, "message 1"},
		},
		{
			"config",
			ErrToBytes(Error{0x2300, "message 1"}),
			Error{frame.ErrCodeConfig, "message 1"},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			buf.Write(tc.content)
			out := ParseError(&buf)
			if diff := cmp.Diff(out, tc.expected); diff != "" {
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
			"unavailable",
			frame.MassAppendBytes(ErrToBytes(Error{0x1000, "message 2"}),
				frame.ShortToBytes(frame.Consistency(1)),
				frame.IntToBytes(frame.Int(2)),
				frame.IntToBytes(frame.Int(3))),
			UnavailableError{
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
			if diff := cmp.Diff(out, tc.expected); diff != "" {
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
			"write timeout",
			frame.MassAppendBytes(ErrToBytes(Error{0x1100, "message 2"}),
				frame.ShortToBytes(frame.Short(0x0004)),
				frame.IntToBytes(frame.Int(-5)),
				frame.IntToBytes(frame.Int(100)),
				frame.StringToBytes("SIMPLE")),
			WriteTimeoutError{
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
			if out != tc.expected {
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
			"write timeout",
			frame.MassAppendBytes(ErrToBytes(Error{0x1200, "message 2"}),
				frame.ShortToBytes(frame.Short(0x0002)),
				frame.IntToBytes(frame.Int(8)),
				frame.IntToBytes(frame.Int(32)),
				frame.ByteToBytes(0)),
			ReadTimeoutError{
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
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing 'WriteTo Timeout' error.")
			}
		})
	}
}

func TestReadFailureError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected ReadFailureError
	}{
		{
			"write timeout",
			frame.MassAppendBytes(ErrToBytes(Error{0x1300, "message 2"}),
				frame.ShortToBytes(frame.Short(0x0003)),
				frame.IntToBytes(frame.Int(4)),
				frame.IntToBytes(frame.Int(5)),
				frame.IntToBytes(frame.Int(6)),
				frame.ByteToBytes(123)),
			ReadFailureError{
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
			if diff := cmp.Diff(out, tc.expected); diff != "" {
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
			"write timeout",
			frame.MassAppendBytes(ErrToBytes(Error{0x1400, "message 2"}),
				frame.StringToBytes("keyspace_name"),
				frame.StringToBytes("function_name"),
				frame.StringListToBytes([]string{"type1", "type2"})),
			FuncFailureError{
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
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal("Failure while constructing 'Function Failure' error.")
			}
		})
	}
}

func TestWriteFailureError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected WriteFailureError
	}{
		{
			"write timeout",
			frame.MassAppendBytes(ErrToBytes(Error{0x1500, "message 2"}),
				frame.ShortToBytes(0x0000),
				frame.IntToBytes(2),
				frame.IntToBytes(4),
				frame.IntToBytes(8),
				frame.StringToBytes("COUNTER")),
			WriteFailureError{
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
			if diff := cmp.Diff(out, tc.expected); diff != "" {
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
			"write timeout",
			frame.MassAppendBytes(ErrToBytes(Error{0x2400, "message 2"}),
				frame.StringToBytes("keyspace_name"),
				frame.StringToBytes("table_name")),
			AlreadyExistsError{
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
			"write timeout",
			frame.MassAppendBytes(ErrToBytes(Error{0x2500, "message 2"}),
				frame.BytesToShortBytes([]byte{1, 2, 3})),
			UnpreparedError{
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
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
