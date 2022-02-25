package transport

import (
	"testing"

	"go.uber.org/goleak"
)

const TestHost = "192.168.100.100"

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
