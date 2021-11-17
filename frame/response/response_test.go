package response

import (
	"bytes"
	"fmt"
	"reflect"
	"scylla-go-driver/frame"
	"testing"
)

// ------------------------------- ERROR TESTS --------------------------------

func ErrToBytes(err Error) []byte {
	var out bytes.Buffer
	frame.WriteInt(err.code, &out)
	frame.WriteString(err.message, &out)
	return out.Bytes()
}

func ShortToBytes(x frame.Short) []byte {
	var out bytes.Buffer
	frame.WriteShort(x, &out)
	return out.Bytes()
}

func IntToBytes(x frame.Int) []byte {
	var out bytes.Buffer
	frame.WriteInt(x, &out)
	return out.Bytes()
}

func massAppendBytes(elems ...[]byte) []byte {
	var ans []byte
	for _, v := range elems {
		ans = append(ans, v...)
	}
	return ans
}

func TestValidErrorCodes(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected Error
	}{
		{"server",
			ErrToBytes(Error{0x0000, "message 1"}),
			Error{server, "message 1"},
		},
		{"protocol",
			ErrToBytes(Error{0x000a, "message 1"}),
			Error{protocol, "message 1"},
		},
		{"authentication",
			ErrToBytes(Error{0x0100, "message 1"}),
			Error{auth, "message 1"},
		},
		{"overload",
			ErrToBytes(Error{0x1001, "message 1"}),
			Error{overload, "message 1"},
		},
		{"is_bootstrapping",
			ErrToBytes(Error{0x1002, "message 1"}),
			Error{bootstrap, "message 1"},
		},
		{"truncate",
			ErrToBytes(Error{0x1003, "message 1"}),
			Error{truncate, "message 1"},
		},
		{"syntax",
			ErrToBytes(Error{0x2000, "message 1"}),
			Error{syntax, "message 1"},
		},
		{"unauthorized",
			ErrToBytes(Error{0x2100, "message 1"}),
			Error{unauthorized, "message 1"},
		},
		{"invalid",
			ErrToBytes(Error{0x2200, "message 1"}),
			Error{invalid, "message 1"},
		},
		{"config",
			ErrToBytes(Error{0x2300, "message 1"}),
			Error{config, "message 1"},
		},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Short reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)
			out := ReadError(&buf)

			if out != tc.expected {
				t.Fatal("Failure while constructing 'Unavailable' error.")
			}
		})

		buf.Reset()
	}
}

func TestUnavailableErr(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected UnavailableErr
	}{
		{"unavailable",
			massAppendBytes(ErrToBytes(Error{0x1000, "message 2"}),
				ShortToBytes(frame.Short(1)),
				IntToBytes(frame.Int(2)),
				IntToBytes(frame.Int(3))),
			UnavailableErr{
				Error{0x1000, "message 2"}, 1, 2, 3,
			},
		},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Short reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)
			out := ReadUnavailable(&buf)

			if out != tc.expected {
				t.Fatal("Failure while constructing 'Unavailable' error.")
			}
		})

		buf.Reset()
	}
}

func StringToBytes(x string) []byte {
	var out bytes.Buffer
	frame.WriteString(x, &out)
	return out.Bytes()
}

func TestWriteTimeoutErr(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected WriteTimeoutErr
	}{
		{"write timeout",
			massAppendBytes(ErrToBytes(Error{0x1100, "message 2"}),
				ShortToBytes(frame.Short(0x0004)),
				IntToBytes(frame.Int(-5)),
				IntToBytes(frame.Int(100)),
				StringToBytes("SIMPLE")),
			WriteTimeoutErr{
				Error{0x1100, "message 2"}, 0x0004, -5, 100, "SIMPLE",
			},
		},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Short reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)
			out := ReadWriteTimeout(&buf)

			if out != tc.expected {
				t.Fatal("Failure while constructing 'Write Timeout' error.")
			}
		})

		buf.Reset()
	}
}

func ByteToBytes(b frame.Byte) []byte {
	var out bytes.Buffer
	frame.WriteByte(b, &out)
	return out.Bytes()
}

func TestReadTimeoutErr(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected ReadTimeoutErr
	}{
		{"write timeout",
			massAppendBytes(ErrToBytes(Error{0x1200, "message 2"}),
				ShortToBytes(frame.Short(0x0002)),
				IntToBytes(frame.Int(8)),
				IntToBytes(frame.Int(32)),
				ByteToBytes(0)),
			ReadTimeoutErr{
				Error{0x1200, "message 2"}, 0x0002, 8, 32, 0,
			},
		},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Short reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)
			out := ReadRTimeout(&buf)

			if out != tc.expected {
				t.Fatal("Failure while constructing 'Write Timeout' error.")
			}
		})

		buf.Reset()
	}
}

func TestReadFailure(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected ReadFailureErr
	}{
		{"write timeout",
			massAppendBytes(ErrToBytes(Error{0x1300, "message 2"}),
				ShortToBytes(frame.Short(0x0003)),
				IntToBytes(frame.Int(4)),
				IntToBytes(frame.Int(5)),
				IntToBytes(frame.Int(6)),
				ByteToBytes(123)),
			ReadFailureErr{
				Error{0x1300, "message 2"}, 0x0003, 4, 5, 6, 123,
			},
		},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Short reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)
			out := ReadRFailure(&buf)

			if out != tc.expected {
				t.Fatal("Failure while constructing 'Write Timeout' error.")
			}
		})

		buf.Reset()
	}
}

func StringListToBytes(sl frame.StringList) []byte {
	var out bytes.Buffer
	frame.WriteStringList(sl, &out)
	return out.Bytes()
}

func TestFuncFailure(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected FuncFailureErr
	}{
		{"write timeout",
			massAppendBytes(ErrToBytes(Error{0x1400, "message 2"}),
				StringToBytes("keyspace_name"),
				StringToBytes("function_name"),
				StringListToBytes([]string{"type1", "type2"})),
			FuncFailureErr{
				Error{0x1400, "message 2"}, "keyspace_name", "function_name", []string{"type1", "type2"},
			},
		},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Short reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)
			out := ReadFuncFailure(&buf)

			// TODO are reflections in tests permitted?
			if !reflect.DeepEqual(out, tc.expected) {
				t.Fatal("Failure while constructing 'Function Failure' error.")
			}
		})

		buf.Reset()
	}
}

func TestWriteFailure(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected WriteFailureErr
	}{
		{"write timeout",
			massAppendBytes(ErrToBytes(Error{0x1500, "message 2"}),
				ShortToBytes(0x0000),
				IntToBytes(2),
				IntToBytes(4),
				IntToBytes(8),
				StringToBytes("COUNTER")),
			WriteFailureErr{
				Error{0x1500, "message 2"}, 0x0000, 2, 4, 8, "COUNTER",
			},
		},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Short reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)
			out := ReadWriteFailure(&buf)

			if out != tc.expected {
				t.Fatal("Failure while constructing 'Function Failure' error.")
			}
		})

		buf.Reset()
	}
}

func TestAlreadyExists(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected AlreadyExistsErr
	}{
		{"write timeout",
			massAppendBytes(ErrToBytes(Error{0x2400, "message 2"}),
				StringToBytes("keyspace_name"),
				StringToBytes("table_name")),
			AlreadyExistsErr{
				Error{0x2400, "message 2"}, "keyspace_name", "table_name",
			},
		},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Short reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)
			out := ReadAlreadyExists(&buf)

			if out != tc.expected {
				t.Fatal("Failure while constructing 'Function Failure' error.")
			}
		})

		buf.Reset()
	}
}

func BytesToBytes(b frame.Bytes) []byte {
	var out bytes.Buffer
	frame.WriteBytes(b, &out)
	return out.Bytes()
}

// There are no tests for unprepared error in rust nor java
func TestUnprepared(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected UnpreparedErr
	}{
		{"write timeout",
			massAppendBytes(ErrToBytes(Error{0x2500, "message 2"}),
				BytesToBytes([]byte{1, 2, 3})),
			UnpreparedErr{
				Error{0x2500, "message 2"}, []byte{1, 2, 3},
			},
		},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Short reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)
			out := ReadUnprepared(&buf)

			// TODO are reflections in tests permitted?
			if !reflect.DeepEqual(out, tc.expected) {
				t.Fatal("Failure while constructing 'Function Failure' error.")
			}
		})

		buf.Reset()
	}
}

// ------------------------------- AUTHENTICATE TESTS --------------------------------

func TestAuthenticateEncodeDecode(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected string
	}{
		{"Mock authenticator",
			[]byte{0x00, 0x11, 0x4d, 0x6f, 0x63, 0x6b, 0x41, 0x75, 0x74, 0x68, 0x65, 0x6e, 0x74, 0x69, 0x63, 0x61, 0x74, 0x6f, 0x72},
			"MockAuthenticator",
		},

	}

	var out bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("AuthResponse Test %s", tc.name), func(t *testing.T) {
			out.Write(tc.content)
			a := ReadAuthenticate(&out)
			if a.Name != tc.expected {
				t.Fatal("Failure while encoding and decoding Authenticate.")
			}

			if out.Len() != 0 {
				t.Fatal ("Failure buffer not empty after read.")
			}
		})
	}
}