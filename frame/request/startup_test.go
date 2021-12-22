package request

import (
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestWriteStartup(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		content Startup
	}{
		{
			"mandatory only",
			Startup{
				Options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
				},
			},
		},
		{
			"compression",
			Startup{
				Options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"COMPRESSION": "lz4",
				},
			},
		},
	}
	for i := 0; i < len(cases); i++ {
		v := cases[i]
		t.Run(v.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			v.content.WriteTo(&buf)
			readOptions := buf.ReadStartupOptions()
			if diff := cmp.Diff(readOptions, v.content.Options); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
