package transport

import (
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
			error:    &SyntaxError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "Invalid",
			error:    &InvalidError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "AlreadyExists",
			error:    &AlreadyExistsError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "FuncFailure",
			error:    &FuncFailureError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "Credentials",
			error:    &CredentialsError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "Unauthorized",
			error:    &UnauthorizedError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "Config",
			error:    &ConfigError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "ReadFailure",
			error:    &FuncFailureError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name: "ReadFailure",
			error: &ReadFailureError{
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
			error: &WriteFailureError{
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
			error:    &UnpreparedError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: DontRetry,
			resIdem2: DontRetry,
		},
		{
			name:     "Overloaded",
			error:    &OverloadedError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: RetryNextNode,
			resIdem2: RetryNextNode,
		},
		{
			name:     "Truncate",
			error:    &TruncateError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: RetryNextNode,
			resIdem2: RetryNextNode,
		},
		{
			name:     "Server",
			error:    &ServerError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: RetryNextNode,
			resIdem2: RetryNextNode,
		},
		{
			name:     "IO",
			error:    &IoError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: RetryNextNode,
			resIdem2: RetryNextNode,
		},
		{
			name:     "IO",
			error:    &IoError{},
			res1:     DontRetry,
			res2:     DontRetry,
			resIdem1: RetryNextNode,
			resIdem2: RetryNextNode,
		},
		{
			name:     "IsBootstrapping",
			error:    &IsBootstrappingError{},
			res1:     RetryNextNode,
			res2:     RetryNextNode,
			resIdem1: RetryNextNode,
			resIdem2: RetryNextNode,
		},
		{
			name: "Unavailable",
			error: &UnavailableError{
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
			error: &ReadTimeoutError{
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
			error: &ReadTimeoutError{
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
			error: &ReadTimeoutError{
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
			error: &WriteTimeoutError{
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
			error: &WriteTimeoutError{
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
