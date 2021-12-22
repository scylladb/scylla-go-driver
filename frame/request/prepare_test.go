package request

import (
	"scylla-go-driver/frame"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestPrepare(t *testing.T) {
	var testCases = []struct {
		name     string
		content  Prepare
		expected []byte
	}{
		{"SELECT", Prepare{"SELECT * FROM foo"}, frame.LongStringToBytes("SELECT * FROM foo")},
	}
	t.Parallel()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := frame.Buffer{}
			tc.content.WriteTo(&b)
			if diff := cmp.Diff(tc.expected, b.Bytes()); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
