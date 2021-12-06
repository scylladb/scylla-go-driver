package request

import (
	"github.com/google/go-cmp/cmp"
	"scylla-go-driver/frame"
	"testing"
)

func TestWriteStartup(t *testing.T) {
	var cases = []struct {
		name    string
		content Startup
	}{
		{"mandatory only",
			Startup{
				Options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
				},
			},
		},
		{"compression",
			Startup{
				Options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"COMPRESSION": "lz4",
				},
			},
		},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			var buf frame.Buffer
			v.content.WriteTo(&buf)
			readOptions := buf.ReadStartupOptions()
			if diff := cmp.Diff(readOptions, v.content.Options); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
