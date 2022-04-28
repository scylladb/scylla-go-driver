package transport

import (
	"fmt"
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
	. "github.com/mmatczuk/scylla-go-driver/frame/response"
)

func TestDefaultRetryPolicy(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		error    error
		res1     RetryDecision
		res2     RetryDecision
		resIdem1 RetryDecision
		resIdem2 RetryDecision
	}{
		{
			name:     "Syntax",
			error:    ScyllaError{Code: frame.ErrCodeSyntax},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "Invalid",
			error:    ScyllaError{Code: frame.ErrCodeInvalid},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "AlreadyExists",
			error:    AlreadyExistsError{ScyllaError: ScyllaError{Code: frame.ErrCodeAlreadyExists}},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "FuncFailure",
			error:    FuncFailureError{ScyllaError: ScyllaError{Code: frame.ErrCodeFunctionFailure}},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "Credentials",
			error:    ScyllaError{Code: frame.ErrCodeCredentials},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "Unauthorized",
			error:    ScyllaError{Code: frame.ErrCodeUnauthorized},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "Config",
			error:    ScyllaError{Code: frame.ErrCodeConfig},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name: "ReadFailure",
			error: ReadFailureError{
				ScyllaError: ScyllaError{Code: frame.ErrCodeReadFailure},
				Consistency: frame.TWO,
				Received:    2,
				BlockFor:    1,
				NumFailures: 1,
				DataPresent: false,
			},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name: "WriteFailure",
			error: WriteFailureError{
				ScyllaError: ScyllaError{Code: frame.ErrCodeWriteFailure},
				Consistency: frame.TWO,
				Received:    1,
				BlockFor:    2,
				NumFailures: 1,
				WriteType:   frame.BatchLog,
			},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "Unprepared",
			error:    UnpreparedError{ScyllaError: ScyllaError{Code: frame.ErrCodeUnprepared}},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "Overloaded",
			error:    ScyllaError{Code: frame.ErrCodeOverloaded},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: RetryNextNode,
			resIdem2: RetryNextNode,
		},
		{
			name:     "Truncate",
			error:    ScyllaError{Code: frame.ErrCodeTruncate},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: RetryNextNode,
			resIdem2: RetryNextNode,
		},
		{
			name:     "Server",
			error:    ScyllaError{Code: frame.ErrCodeServer},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: RetryNextNode,
			resIdem2: RetryNextNode,
		},
		{
			name:     "IO",
			error:    fmt.Errorf("dummy error"),
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: RetryNextNode,
			resIdem2: RetryNextNode,
		},
		{
			name:     "IsBootstrapping",
			error:    ScyllaError{Code: frame.ErrCodeBootstrapping},
			res1:     RetryNextNode,
			res2:     RetryNextNode,
			resIdem1: RetryNextNode,
			resIdem2: RetryNextNode,
		},
		{
			name: "Unavailable",
			error: UnavailableError{
				ScyllaError: ScyllaError{Code: frame.ErrCodeUnavailable},
				Consistency: frame.TWO,
				Required:    2,
				Alive:       1,
			},
			res1:     RetryNextNode,
			res2:     DontRetry,
			resIdem1: RetryNextNode,
			resIdem2: DontRetry,
		},
		{
			name: "ReadTimeout enough responses, data == true",
			error: ReadTimeoutError{
				ScyllaError: ScyllaError{Code: frame.ErrCodeReadTimeout},
				Consistency: frame.TWO,
				Received:    2,
				BlockFor:    2,
				DataPresent: true,
			},
			res1:     RetrySameNode,
			res2:     DontRetry,
			resIdem1: RetrySameNode,
			resIdem2: DontRetry,
		},
		{
			name: "ReadTimeout enough responses, data == false",
			error: ReadTimeoutError{
				ScyllaError: ScyllaError{Code: frame.ErrCodeReadTimeout},
				Consistency: frame.TWO,
				Received:    2,
				BlockFor:    2,
				DataPresent: false,
			},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name: "ReadTimeout not enough responses, data == true",
			error: ReadTimeoutError{
				ScyllaError: ScyllaError{Code: frame.ErrCodeReadTimeout},
				Consistency: frame.TWO,
				Received:    1,
				BlockFor:    2,
				DataPresent: true,
			},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name: "WriteTimeout write type == BatchLog",
			error: WriteTimeoutError{
				ScyllaError: ScyllaError{Code: frame.ErrCodeWriteTimeout},
				Consistency: frame.TWO,
				Received:    1,
				BlockFor:    2,
				WriteType:   frame.BatchLog,
			},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: RetrySameNode,
			resIdem2: DontRetry,
		},
		{
			name: "WriteTimeout write type != BatchLog",
			error: WriteTimeoutError{
				ScyllaError: ScyllaError{Code: frame.ErrCodeWriteTimeout},
				Consistency: frame.TWO,
				Received:    4,
				BlockFor:    2,
				WriteType:   frame.Simple,
			},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ri := RetryInfo{
				Error:      tc.error,
				Idempotent: false,
			}

			decider := NewDefaultRetryPolicy().NewRetryDecider()
			res := decider.Decide(ri)
			if res != tc.res1 {
				t.Fatalf("First retry decision for: %+#v, wanted: %v, got: %v", ri, DontRetry, res)
			}
			res = decider.Decide(ri)
			if res != tc.res2 {
				t.Fatalf("Second retry decision for: %+#v, wanted: %v, got: %v", ri, DontRetry, res)
			}

			ri.Idempotent = true

			decider = NewDefaultRetryPolicy().NewRetryDecider()
			res = decider.Decide(ri)
			if res != tc.resIdem1 {
				t.Fatalf("(Idempotent) First retry decision for: %+#v, wanted: %v, got: %v", ri, DontRetry, res)
			}
			res = decider.Decide(ri)
			if res != tc.resIdem2 {
				t.Fatalf("(Idempotent) Second retry decision for: %+#v, wanted: %v, got: %v", ri, DontRetry, res)
			}
		})
	}
}
