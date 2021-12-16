package request

import (
	"github.com/google/go-cmp/cmp"
	"scylla-go-driver/frame"
	"testing"
)

func TestPrepare(t *testing.T) {
	var cases = []struct {
		name     string
		content  Prepare
		expected []byte
	}{
		{"SELECT", Prepare{"SELECT * FROM foo"}, frame.LongStringToBytes("SELECT * FROM foo")},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			b := frame.Buffer{}
			v.content.WriteTo(&b)
			if diff := cmp.Diff(v.expected, b.Bytes()); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
