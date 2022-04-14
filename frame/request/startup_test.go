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

// We want to make sure that parsing does not crush driver even for random data.
func FuzzStartup(f *testing.F) {
	f.Fuzz(func(t *testing.T, s1, s2, s3, s4, s5, s6 string) { // nolint:thelper // This is not a helper function.
		in := Startup{Options: frame.StartupOptions{s1: s2, s3: s4, s5: s6}}
		var buf frame.Buffer
		in.WriteTo(&buf)
		if buf.Error() != nil {
			t.Error(buf.Error())
		}
	})
}
