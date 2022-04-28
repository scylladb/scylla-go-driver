package frame

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

// Fuzzing tests are based on:
//   - data            == deserialize(serialize(data))
//   - serialize(data) == serialize(deserialize(serialize(data)))
// with the exception of comparing serialized maps, which is nondeterministic because of
// nondeterministic iteration over build-in map.
//
// command to use fuzzing: go test -fuzz=XXX -fuzztime 30s
// where XXX is unambiguous fuzz test name.

func FuzzBufferByte(f *testing.F) {
	testCases := []Byte{0, 22, 125, 255}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, in Byte) {
		var buf Buffer
		buf.WriteByte(in)
		inBuf := make([]byte, len(buf.Bytes()))
		copy(inBuf, buf.Bytes())
		out := buf.ReadByte()
		if in != out || buf.Error() != nil {
			t.Errorf("in: %v, out: %v, err: %v", in, out, buf.Error())
		}
		buf.WriteByte(out)
		outBuf := buf.Bytes()
		if diff := cmp.Diff(inBuf, outBuf); diff != "" || buf.Error() != nil {
			t.Errorf("inBuff: %v, outBuff: %v, err: %v", inBuf, outBuf, buf.Error())
		}
	})
}

func FuzzBufferShort(f *testing.F) {
	testCases := []Short{0, 245, 42995, 65535}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, in Short) {
		var buf Buffer
		buf.WriteShort(in)
		inBuf := make([]byte, len(buf.Bytes()))
		copy(inBuf, buf.Bytes())
		out := buf.ReadShort()
		if in != out || buf.Error() != nil {
			t.Errorf("in: %v, out: %v, err: %v", in, out, buf.Error())
		}
		buf.WriteShort(out)
		outBuf := buf.Bytes()
		if diff := cmp.Diff(inBuf, outBuf); diff != "" || buf.Error() != nil {
			t.Errorf("inBuff: %v, outBuff: %v, err: %v", inBuf, outBuf, buf.Error())
		}
	})
}

func FuzzBufferInt(f *testing.F) {
	testCases := []Int{-2147483648, 0, 1, 9452, 123335, 2147483647}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, in Int) {
		var buf Buffer
		buf.WriteInt(in)
		inBuf := make([]byte, len(buf.Bytes()))
		copy(inBuf, buf.Bytes())
		out := buf.ReadInt()
		if in != out || buf.Error() != nil {
			t.Errorf("in: %v, out: %v, err: %v", in, out, buf.Error())
		}
		buf.WriteInt(out)
		outBuf := buf.Bytes()
		if diff := cmp.Diff(inBuf, outBuf); diff != "" || buf.Error() != nil {
			t.Errorf("inBuff: %v, outBuff: %v, err: %v", inBuf, outBuf, buf.Error())
		}
	})
}

func FuzzBufferString(f *testing.F) {
	testCases := []string{"a", "golang", "πœę©ß", ""}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, in string) {
		var buf Buffer
		buf.WriteString(in)
		inBuf := make([]byte, len(buf.Bytes()))
		copy(inBuf, buf.Bytes())
		out := buf.ReadString()
		if in != out || buf.Error() != nil {
			t.Errorf("in: %v, out: %v, err: %v", in, out, buf.Error())
		}
		buf.WriteString(out)
		outBuf := buf.Bytes()
		if diff := cmp.Diff(inBuf, outBuf); diff != "" || buf.Error() != nil {
			t.Errorf("inBuff: %v, outBuff: %v, err: %v", inBuf, outBuf, buf.Error())
		}
	})
}

func FuzzBufferStringList(f *testing.F) {
	testCases := [][3]string{{"a", "", ""}, {"a", "b", ""}}
	for _, tc := range testCases {
		f.Add(tc[0], tc[1], tc[2])
	}
	f.Fuzz(func(t *testing.T, a, b, c string) {
		in := StringList{a, b, c}
		var buf Buffer
		buf.WriteStringList(in)
		inBuf := make([]byte, len(buf.Bytes()))
		copy(inBuf, buf.Bytes())
		out := buf.ReadStringList()
		if diff := cmp.Diff(in, out); diff != "" || buf.Error() != nil {
			t.Errorf("in: %v, out: %v, err: %v", in, out, buf.Error())
		}
		buf.WriteStringList(out)
		outBuf := buf.Bytes()
		if diff := cmp.Diff(inBuf, outBuf); diff != "" || buf.Error() != nil {
			t.Errorf("inBuff: %v, outBuff: %v, err: %v", inBuf, outBuf, buf.Error())
		}
	})
}

func FuzzBufferStringMultiMap(f *testing.F) {
	testCases := [][5]string{{"k1", "v11", "v12", "k2", "v2"}}
	for _, tc := range testCases {
		f.Add(tc[0], tc[1], tc[2], tc[3], tc[4])
	}
	f.Fuzz(func(t *testing.T, k1, v11, v12, k2, v2 string) {
		in := StringMultiMap{k1: {v11, v12}, k2: {v2}}
		var buf Buffer
		buf.WriteStringMultiMap(in)
		out := buf.ReadStringMultiMap()
		if diff := cmp.Diff(in, out); diff != "" || buf.Error() != nil {
			t.Errorf("in: %v, out: %v, err: %v", in, out, buf.Error())
		}
	})
}

func FuzzBufferUUID(f *testing.F) {
	testCases := []UUID{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}}
	for _, tc := range testCases {
		f.Add(tc[:])
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var in UUID
		var buf Buffer
		copy(in[:], data)
		buf.WriteUUID(in)
		inBuf := make([]byte, len(buf.Bytes()))
		copy(inBuf, buf.Bytes())
		out := buf.ReadUUID()
		if diff := cmp.Diff(in, out); diff != "" || buf.Error() != nil {
			t.Errorf("in: %v, out: %v, err: %v", in, out, buf.Error())
		}
		buf.WriteUUID(out)
		outBuf := buf.Bytes()
		if diff := cmp.Diff(inBuf, outBuf); diff != "" || buf.Error() != nil {
			t.Errorf("inBuff: %v, outBuff: %v, err: %v", inBuf, outBuf, buf.Error())
		}
	})
}

func FuzzBufferHeader(f *testing.F) {
	testCases := []Header{{
		Version:  CQLv4,
		Flags:    0,
		StreamID: 0,
		OpCode:   OpSupported,
		Length:   0,
	}}
	for _, tc := range testCases {
		f.Add(tc.Version, tc.Flags, tc.StreamID, tc.OpCode, tc.Length)
	}
	f.Fuzz(func(t *testing.T, v, f byte, s int16, o byte, l int32) {
		in := Header{
			Version:  v,
			Flags:    f,
			StreamID: s,
			OpCode:   o,
			Length:   l,
		}
		var buf Buffer
		in.WriteTo(&buf)
		inBuf := make([]byte, len(buf.Bytes()))
		copy(inBuf, buf.Bytes())
		out := ParseHeader(&buf)
		if diff := cmp.Diff(in, out); diff != "" || buf.Error() != nil {
			t.Errorf("in: %+#v, out: %+#v, err: %v", in, out, buf.Error())
		}
		out.WriteTo(&buf)
		outBuf := buf.Bytes()
		if diff := cmp.Diff(inBuf, outBuf); diff != "" || buf.Error() != nil {
			t.Errorf("inBuff: %v, outBuff: %v, err: %v", inBuf, outBuf, buf.Error())
		}
	})
}
