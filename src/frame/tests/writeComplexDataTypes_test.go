package tests

import (
	"scylla-go-driver/src/frame"
	"testing"
)

var stringListTests = []struct {
	strLst []string
}{
	{[]string{"a", "b", "c"}},
	{[]string{"golang", "is", "awesome", "!"}},
	{[]string{"ąśðąśπœ©ąąśð\n\r\nśąðąś", "sadasd", "\a\b\\c\\d\\e\f\\g"}},
}

func TestWriteStringList(t *testing.T) {
	buf := new([]byte)

	for _, v := range stringListTests {
		frame.SWriteStringList(v.strLst, buf)
		length, err := frame.ReadShort(buf)

		if err != nil {
			t.Errorf("ReadShort error: %s", err.Error())
		}

		if int(length) != len(v.strLst) {
			t.Errorf("Wrong string list length. Got %v, expected %v.", length, len(v.strLst))
		}

		for _, v := range v.strLst {
			str, err := frame.ReadString(buf)

			if err != nil {
				t.Errorf("ReadString error: %s", err.Error())
			}

			if v != str {
				t.Errorf("Wrong result string. Got %v, expected %v.", str, v)
			}
		}

		*buf = (*buf)[:0]
	}
}
