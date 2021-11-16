package request

import (
	"bytes"
	"fmt"
	"scylla-go-driver/frame"
	"testing"
)

// ------------------------------- STARTUP TESTS -----------------------------

func StringMapEqual(a, b frame.StringMap) bool {
	for v, k := range a {
		if mv, ok := b[v]; !(ok && mv == k) {
			return false
		}
	}
	for v, k := range b {
		if mv, ok := a[v]; !(ok && mv == k) {
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
		{"mandatory options only",
			Startup{
			options: frame.StringMap{
				"CQL_VERSION": "3.0.0",
				},
			},
		},
		{"mandatory + some possible options",
			Startup{
				options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"COMPRESSION": "lz4",
					"THROW_ON_OVERLOAD": "",
				},
			},
		},
		{"all possible options",
			Startup{
				options: frame.StringMap{
					"CQL_VERSION": "3.0.0",
					"COMPRESSION": "snappy",
					"THROW_ON_OVERLOAD": "",
					"NO_COMPACT": "",
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