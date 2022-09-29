package gocql

import (
	"math"
	"math/rand"
	"time"

	"github.com/scylladb/scylla-go-driver/transport"
)

// ExponentialBackoffRetryPolicy sleeps between attempts
type ExponentialBackoffRetryPolicy struct {
	NumRetries int
	attempts   int
	Min, Max   time.Duration
}

func (e *ExponentialBackoffRetryPolicy) NewRetryDecider() transport.RetryDecider {
	return e
}

func (e *ExponentialBackoffRetryPolicy) Decide(transport.RetryInfo) transport.RetryDecision {
	if e.attempt() {
		return transport.RetryNextNode
	}
	return transport.DontRetry
}

func (e *ExponentialBackoffRetryPolicy) Reset() {
	e.attempts = 0
}

func (e *ExponentialBackoffRetryPolicy) attempt() bool {
	if e.attempts > e.NumRetries {
		return false
	}
	time.Sleep(e.napTime(e.attempts))
	e.attempts++
	return true
}

func (e *ExponentialBackoffRetryPolicy) napTime(attempts int) time.Duration {
	return getExponentialTime(e.Min, e.Max, attempts)
}

// used to calculate exponentially growing time
func getExponentialTime(min time.Duration, max time.Duration, attempts int) time.Duration {
	if min <= 0 {
		min = 100 * time.Millisecond
	}
	if max <= 0 {
		max = 10 * time.Second
	}
	minFloat := float64(min)
	napDuration := minFloat * math.Pow(2, float64(attempts-1))
	// add some jitter
	napDuration += rand.Float64()*minFloat - (minFloat / 2)
	if napDuration > float64(max) {
		return time.Duration(max)
	}
	return time.Duration(napDuration)
}

func transformRetryPolicy(rp RetryPolicy) transport.RetryPolicy {
	if ret, ok := rp.(transport.RetryPolicy); ok {
		return ret
	}
	if rp == nil {
		return transport.NewFallthroughRetryPolicy()
	}

	return transport.NewDefaultRetryPolicy()
}

type SimpleRetryPolicy struct {
	NumRetries int
}
