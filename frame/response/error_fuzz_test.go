package response

import (
	"testing"

	"github.com/scylladb/scylla-go-driver/frame"
)

var (
	errSE  ScyllaError
	errUAE UnavailableError
	errWTE WriteTimeoutError
	errRTE ReadTimeoutError
	errRFE ReadFailureError
	errFFE FuncFailureError
	errWFE WriteFailureError
	errAEE AlreadyExistsError
	errUPE UnpreparedError
)

// We assign the result to a global variable to avoid compiler optimization.
func FuzzScyllaError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseScyllaError(&buf)
		errSE = out
	})
}

func FuzzUnavailableError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseUnavailableError(&buf, ScyllaError{})
		errUAE = out
	})
}

func FuzzWriteTimeoutErrorError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseWriteTimeoutError(&buf, ScyllaError{})
		errWTE = out
	})
}

func FuzzReadTimeoutError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseReadTimeoutError(&buf, ScyllaError{})
		errRTE = out
	})
}

func FuzzReadFailureErrorError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseReadFailureError(&buf, ScyllaError{})
		errRFE = out
	})
}

func FuzzFuncFailureError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseFuncFailureError(&buf, ScyllaError{})
		errFFE = out
	})
}

func FuzzWriteFailureError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseWriteFailureError(&buf, ScyllaError{})
		errWFE = out
	})
}

func FuzzParseAlreadyExistsError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseAlreadyExistsError(&buf, ScyllaError{})
		errAEE = out
	})
}

func FuzzUnpreparedError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
		var buf frame.Buffer
		buf.Write(data)
		out := ParseUnpreparedError(&buf, ScyllaError{})
		errUPE = out
	})
}
