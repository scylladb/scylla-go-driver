package main

import (
	"bytes"
	"scylla-go-driver/src/frame"
)

func testWriteStringMultiMap() {
	var m = map[string][]string{
		"GOLang": {
			"is", "super", "awesome!",
		},
		"Pets": {
			"cat", "dog",
		},
	}
	buf := new(bytes.Buffer)
	_, err := frame.WriteStringMultiMap(m, buf)
	if err != nil {
		return
	}


}

func main() {

}