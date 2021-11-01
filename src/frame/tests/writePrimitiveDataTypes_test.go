package tests

import (
	"bytes"
	"scylla-go-driver/src/frame"
	"testing"
)

var byteTests = []struct {
	nr       byte
	expected []byte
}{
	{0, []byte{0x0}},
	{1, []byte{0x01}},
	{173, []byte{0xad}},
	{255, []byte{0xff}},
}

func TestWriteByte(t *testing.T) {
	buf := new([]byte)

	for _, v := range byteTests {
		frame.SWriteByte(v.nr, buf)

		if !bytes.Equal(*buf, v.expected) {
			t.Errorf("Wrong result buffer. Got %v, expected %v.", *buf, v.expected)
		}

		*buf = (*buf)[:0]
	}
}

var shortTests = []struct {
	nr       uint16
	expected []byte
}{
	{5, []byte{0x0, 0x5}},
	{7919, []byte{0x1e, 0xef}},
	{255, []byte{0x0, 0xff}},
	{256, []byte{0x01, 0x00}},
	{65535, []byte{0xff, 0xff}},
}

func TestWriteShort(t *testing.T) {
	buf := new([]byte)

	for _, v := range shortTests {
		frame.SWriteShort(v.nr, buf)

		if !bytes.Equal(*buf, v.expected) {
			t.Errorf("Wrong result buffer. Got %v, expected %v.", *buf, v.expected)
		}

		*buf = (*buf)[:0]
	}
}

var intTests = []struct {
	nr       int32
	expected []byte
}{
	{0, []byte{0x0, 0x0, 0x0, 0x0}},
	{1, []byte{0x0, 0x0, 0x0, 0x01}},
	{5, []byte{0x0, 0x0, 0x0, 0x05}},
	{9452, []byte{0x0, 0x0, 0x24, 0xec}},
	{123335, []byte{0x0, 0x01, 0xe1, 0xc7}},
	{9075221, []byte{0x0, 0x8a, 0x7a, 0x15}},
}

func TestWriteInt(t *testing.T) {
	buf := new([]byte)

	for _, v := range intTests {
		frame.SWriteInt(v.nr, buf)

		if !bytes.Equal(*buf, v.expected) {
			t.Errorf("Wrong result buffer. Got %v, expected %v.", *buf, v.expected)
		}

		*buf = (*buf)[:0]
	}
}

var stringTests = []string{
	"a",
	"golang",
	"πœę©ß←↓→óþąśðæŋ’ə…łżźć„”ńµ",
	"just another test/../..';",
}

func TestWriteString(t *testing.T) {
	buf := new([]byte)

	for _, v := range stringTests {
		frame.SWriteString(v, buf)
		length := int((*buf)[0])<<8 | int((*buf)[1])

		if length != len(v) {
			t.Errorf("Wrong string length. Got %v, expected %v.", length, len(v))
		}
		if !bytes.Equal((*buf)[2:], []byte(v)) {
			t.Errorf("Wrong result buffer. Got %v, expected %v.", *buf, []byte(v))
		}

		*buf = (*buf)[:0]
	}
}
