package response

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

var (
	dummyUAE *UnavailableError
	dummyWTE *WriteTimeoutError
	dummyRTE *ReadTimeoutError
	dummyRFE *ReadFailureError
	dummyFFE *FuncFailureError
	dummyWFE *WriteFailureError
	dummyAEE *AlreadyExistsError
	dummyUPE *UnpreparedError
)

// We want to make sure that parsing does not crush driver even for random data.
// We assign result to global variable to avoid compiler optimization.
func FuzzUnavailableError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseUnavailableError(&buf)
		dummyUAE = out
	})
}

func FuzzWriteTimeoutErrorError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseWriteTimeoutError(&buf)
		dummyWTE = out
	})
}

func FuzzReadTimeoutError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseReadTimeoutError(&buf)
		dummyRTE = out
	})
}

func FuzzReadFailureErrorError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseReadFailureError(&buf)
		dummyRFE = out
	})
}

func FuzzFuncFailureError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseFuncFailureError(&buf)
		dummyFFE = out
	})
}

func FuzzWriteFailureError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseWriteFailureError(&buf)
		dummyWFE = out
	})
}

func FuzzParseAlreadyExistsError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseAlreadyExistsError(&buf)
		dummyAEE = out
	})
}

func FuzzUnpreparedError(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseUnpreparedError(&buf)
		dummyUPE = out
	})
}
