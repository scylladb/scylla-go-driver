package request

import (
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestPrepare(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		content  Prepare
		expected []byte
	}{
		{"SELECT", Prepare{"SELECT * FROM foo"}, frame.LongStringToBytes("SELECT * FROM foo")},
	}
	for i := 0; i < len(cases); i++ {
		v := cases[i]
		t.Run(v.name, func(t *testing.T) {
			t.Parallel()
			b := frame.Buffer{}
			v.content.WriteTo(&b)
			if diff := cmp.Diff(v.expected, b.Bytes()); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
