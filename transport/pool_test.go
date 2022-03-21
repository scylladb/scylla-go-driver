package transport

import (
	"testing"
)

func TestDefaultPortParsing(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		address  string
		expected string
	}{
		{
			name:     "simple swap",
			address:  "192.168.100.1:8258",
			expected: "192.168.100.1:8258",
		},
		{
			name:     "no port set",
			address:  "192.168.100.1",
			expected: "192.168.100.1:9042",
		},
		{
			name:     "ipv6 with port",
			address:  "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]:8258",
			expected: "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]:8258",
		},
		{
			name:     "ipv6 no port",
			address:  "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]",
			expected: "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]:9042",
		},
		{
			name:     "ipv6 no port no square brackets",
			address:  "2a02:a311:433f:9580:e16:b5d2:6f06:c897",
			expected: "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]:9042",
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if res := withDefaultPort(tc.address); res != tc.expected {
				t.Fatal("Failure while extracting address")
			}
		})
	}
}

func TestCustomPortParsing(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		address  string
		port     string
		expected string
	}{
		{
			name:     "simple swap",
			address:  "192.168.100.1:8258",
			port:     "19042",
			expected: "192.168.100.1:19042",
		},
		{
			name:     "no port set",
			address:  "192.168.100.1",
			port:     "19042",
			expected: "192.168.100.1:19042",
		},
		{
			name:     "ipv6 with port",
			address:  "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]:8258",
			port:     "19042",
			expected: "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]:19042",
		},
		{
			name:     "ipv6 no port",
			address:  "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]",
			port:     "19042",
			expected: "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]:19042",
		},
		{
			name:     "ipv6 no port no square brackets",
			address:  "2a02:a311:433f:9580:e16:b5d2:6f06:c897",
			port:     "19042",
			expected: "[2a02:a311:433f:9580:e16:b5d2:6f06:c897]:19042",
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if res := withPort(tc.address, tc.port); res != tc.expected {
				t.Fatal("Failure while extracting address")
			}
		})
	}
}
