package request

import (
	"bytes"
	"fmt"
	"scylla-go-driver/frame"
	"testing"
)

// ------------------------------- STARTUP TESTS -----------------------------

func StringMapEqual(a, b frame.StringMap) bool {
	for k, v := range a {
		if mv, ok := b[k]; !(ok && mv == v) {
			return false
		}
	}
	for k, v := range b {
		if mv, ok := a[k]; !(ok && mv == v) {
			return false
		}
	}
	return true
}

func TestWriteStartup(t *testing.T) {
	var cases = []struct {
		name     string
		content  Startup
	}{
		{"mandatory only",
			Startup{
				options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
				},
			},
		},
		{"compression",
			Startup{
				options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"COMPRESSION": "lz4",
				},
			},
		},
		{"custom option",
			Startup{
				options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"CUSTOM_OPT1": "VALUE1",
				},
			},
		},
		{"custom option + compression",
			Startup{
				options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"CUSTOM_OPT1": "VALUE1",
					"COMPRESSION": "lz4",
				},
			},
		},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Short writing test %s", tc.name), func(t *testing.T) {
			tc.content.Write(&buf)
			readOptions := frame.ReadStringMap(&buf)

			if !StringMapEqual(readOptions, tc.content.options) {
				t.Fatal("Failure while constructing Startup.")
			}
		})

		buf.Reset()
	}
}