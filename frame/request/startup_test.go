package request

import (
	"scylla-go-driver/frame"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestWriteStartup(t *testing.T) {
	var testCases = []struct {
		name    string
		content Startup
	}{
		{
			name: "mandatory only",
			content: Startup{
				Options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
				},
			},
		},
		{
			name: "compression",
			content: Startup{
				Options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"COMPRESSION": "lz4",
				},
			},
		},
	}
	t.Parallel()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf frame.Buffer
			tc.content.WriteTo(&buf)
			readOptions := buf.ReadStartupOptions()
			if diff := cmp.Diff(readOptions, tc.content.Options); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
