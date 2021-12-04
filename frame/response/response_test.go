package response

import (
	"encoding/hex"
	"github.com/google/go-cmp/cmp"
	"reflect"
	"scylla-go-driver/frame"
	"testing"
)

// ------------------------------- ERROR TESTS --------------------------------

func ErrToBytes(err Error) []byte {
	var out frame.Buffer
	out.WriteInt(err.Code)
	out.WriteString(err.Message)
	return out.Bytes()
}

func ShortToBytes(x frame.Short) []byte {
	var out frame.Buffer
	out.WriteShort(x)
	return out.Bytes()
}

func IntToBytes(x frame.Int) []byte {
	var out frame.Buffer
	out.WriteInt(x)
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
			Error{frame.ErrCodeServer, "message 1"},
		},
		{"protocol",
			ErrToBytes(Error{0x000a, "message 1"}),
			Error{frame.ErrCodeProtocol, "message 1"},
		},
		{"authentication",
			ErrToBytes(Error{0x0100, "message 1"}),
			Error{frame.ErrCodeCredentials, "message 1"},
		},
		{"overload",
			ErrToBytes(Error{0x1001, "message 1"}),
			Error{frame.ErrCodeOverloaded, "message 1"},
		},
		{"is_bootstrapping",
			ErrToBytes(Error{0x1002, "message 1"}),
			Error{frame.ErrCodeBootstrapping, "message 1"},
		},
		{"truncate",
			ErrToBytes(Error{0x1003, "message 1"}),
			Error{frame.ErrCodeTruncate, "message 1"},
		},
		{"syntax",
			ErrToBytes(Error{0x2000, "message 1"}),
			Error{frame.ErrCodeSyntax, "message 1"},
		},
		{"unauthorized",
			ErrToBytes(Error{0x2100, "message 1"}),
			Error{frame.ErrCodeUnauthorized, "message 1"},
		},
		{"invalid",
			ErrToBytes(Error{0x2200, "message 1"}),
			Error{frame.ErrCodeInvalid, "message 1"},
		},
		{"config",
			ErrToBytes(Error{0x2300, "message 1"}),
			Error{frame.ErrCodeConfig, "message 1"},
		},
	}

	var buf frame.Buffer
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out, _ := ParseError(&buf)
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

	var buf frame.Buffer
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out, _ := ParseUnavailable(&buf)
			if out != tc.expected {
				t.Fatal("Failure while constructing 'Unavailable' error.")
			}
		})

		buf.Reset()
	}
}

func StringToBytes(x string) []byte {
	var out frame.Buffer
	out.WriteString(x)
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

	var buf frame.Buffer
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out, _ := ParseWriteTimeout(&buf)
			if out != tc.expected {
				t.Fatal("Failure while constructing 'WriteTo Timeout' error.")
			}
		})
		buf.Reset()
	}
}

func ByteToBytes(b frame.Byte) []byte {
	var out frame.Buffer
	out.WriteByte(b)
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

	var buf frame.Buffer
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out, _ := ParseReadTimeout(&buf)
			if out != tc.expected {
				t.Fatal("Failure while constructing 'WriteTo Timeout' error.")
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

	var buf frame.Buffer
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out, _ := ParseReadFailure(&buf)
			if out != tc.expected {
				t.Fatal("Failure while constructing 'WriteTo Timeout' error.")
			}
		})
		buf.Reset()
	}
}

func StringListToBytes(sl frame.StringList) []byte {
	var out frame.Buffer
	out.WriteStringList(sl)
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

	var buf frame.Buffer
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out, _ := ParseFuncFailure(&buf)
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

	var buf frame.Buffer
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf.Write(tc.content)
			out, _ := ParseWriteFailure(&buf)
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

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf frame.Buffer
			buf.Write(tc.content)
			out, _ := ParseAlreadyExists(&buf)
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func BytesToBytes(b frame.Bytes) []byte {
	var out frame.Buffer
	out.WriteBytes(b)
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

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf frame.Buffer
			buf.Write(tc.content)
			out, _ := ParseUnprepared(&buf)
			if diff := cmp.Diff(out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
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

	var out frame.Buffer
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out.Write(tc.content)
			a, _ := ParseAuthenticate(&out)
			if diff := cmp.Diff(a.Name, tc.expected); diff != "" {
				t.Fatal(diff)
			}
			if len(out.Bytes()) != 0 {
				t.Fatal("Failure buffer not empty after read.")
			}
		})
	}
}

// ------------------------------- AUTH CHALLENGE TESTS --------------------------------

// HexStringToBytes does begin with string's length.
func HexStringToBytes(s string) []byte {
	tmp, _ := hex.DecodeString(s)
	return tmp
}

func TestAuthChallenge(t *testing.T) {
	cases := []struct {
		name     string
		content  []byte
		expected AuthChallenge
	}{
		{"simple",
			massAppendBytes(IntToBytes(frame.Int(4)), HexStringToBytes("cafebabe")),
			AuthChallenge{HexStringToBytes("cafebabe")}},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			var buf frame.Buffer
			buf.Write(v.content)
			a, _ := ParseAuthChallenge(&buf)
			if diff := cmp.Diff(a, v.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

// ------------------------------- SCHEMA CHANGE EVENT TESTS --------------------------------

func TestSchemaChangeEvent(t *testing.T) {
	cases := []struct {
		name     string
		content  []byte
		expected SchemaChange
	}{
		{"KEYSPACE",
			massAppendBytes(StringToBytes("CREATED"),
				StringToBytes("KEYSPACE"),
				StringToBytes("test")),
			SchemaChange{Change: "CREATED", Target: "KEYSPACE", Keyspace: "test"}},
		{"TABLE",
			massAppendBytes(StringToBytes("CREATED"),
				StringToBytes("TABLE"),
				StringToBytes("test"),
				StringToBytes("mytable")),
			SchemaChange{Change: "CREATED",
				Target:   "TABLE",
				Keyspace: "test",
				Object:   "mytable"}},
		{"TYPE",
			massAppendBytes(StringToBytes("CREATED"),
				StringToBytes("TYPE"),
				StringToBytes("test"),
				StringToBytes("mytype")),
			SchemaChange{Change: "CREATED",
				Target:   "TYPE",
				Keyspace: "test",
				Object:   "mytype"}},
		{"FUNCTION",
			massAppendBytes(StringToBytes("CREATED"),
				StringToBytes("FUNCTION"),
				StringToBytes("test"),
				StringToBytes("myfunction"),
				StringListToBytes([]string{"int", "int"})),
			SchemaChange{Change: "CREATED",
				Target:    "FUNCTION",
				Keyspace:  "test",
				Object:    "myfunction",
				Arguments: []string{"int", "int"}}},
		{"AGGREGATE",
			massAppendBytes(StringToBytes("CREATED"),
				StringToBytes("AGGREGATE"),
				StringToBytes("test"),
				StringToBytes("myaggregate"),
				StringListToBytes([]string{"int", "int"})),
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
			s, _ := ParseSchemaChange(&buf)
			if diff := cmp.Diff(s, v.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

// ------------------------------- AUTH SUCCESS TESTS --------------------------------
func TestAuthSuccessEncodeDecode(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected []byte
	}{
		{"Should encode and decode",
			[]byte{0x04, 0x00, 0x00, 0x00, 0xca, 0xfe, 0xba, 0xbe},
			[]byte{0xca, 0xfe, 0xba, 0xbe},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out frame.Buffer
			a, _ := ParseAuthSuccess(&out)
			if diff := cmp.Diff(a.Token, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

// ------------------------------- STATUS CHANGE EVENT TESTS --------------------------------

func InetToBytes(i frame.Inet) []byte {
	b := frame.Buffer{}
	b.WriteInet(i)
	return b.Bytes()
}

func TestStatusChangeEvent(t *testing.T) {
	cases := []struct {
		name     string
		content  []byte
		expected StatusChange
	}{
		{"UP",
			massAppendBytes(StringToBytes("UP"),
				InetToBytes(frame.Inet{
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
			a, _ := ParseStatusChange(&buf)
			if diff := cmp.Diff(a, v.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

// ------------------------------- TOPOLOGY CHANGE EVENT TESTS --------------------------------

func TestTopologyChangeEvent(t *testing.T) {
	cases := []struct {
		name     string
		content  []byte
		expected TopologyChange
	}{
		{"NEW_NODE",
			massAppendBytes(StringToBytes("NEW_NODE"),
				InetToBytes(frame.Inet{
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
			a, _ := ParseTopologyChange(&buf)
			if diff := cmp.Diff(a, v.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
