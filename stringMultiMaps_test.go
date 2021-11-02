package frame

/*
import (
	"scylla-go-driver/src/frame"
	"testing"
)

func TestWriteReadStringMultiMap(t *testing.T) {
	var m = frame.StringMultiMap{
		"GOLang": {
			"is", "super", "awesome!",
		},
		"Pets": {
			"cat", "dog",
		},
	}
	buf := make([]byte, 0, 128)
	frame.WriteStringMultiMap(m, &buf)
	m2 := make(frame.StringMultiMap)
	err := frame.ReadStringMultiMap(&buf, m2)
	if err != nil {
		t.Errorf("Error from ReadStringMultiMap: %s", err.Error())
	}
	if len(buf) != 0 {
		t.Errorf("Buffer should be empty.")
	}
}
*/
