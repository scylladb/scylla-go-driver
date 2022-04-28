package request

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestWriteStartup(t *testing.T) {
	t.Parallel()
	testCases := []struct {
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
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf frame.Buffer
			tc.content.WriteTo(&buf)
			readOptions := buf.ReadStartupOptions()
			if diff := cmp.Diff(readOptions, tc.content.Options); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
