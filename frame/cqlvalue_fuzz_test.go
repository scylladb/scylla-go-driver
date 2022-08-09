package frame

import (
	"math"
	"net"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// Fuzzing tests are based on: data == deserialize(serialize(data))
// We are trying to fuzz only correct data, so that the process of serialization
// and deserialization can be a good indicator of correctness.
// Fuzzing random data is done inside request/response packages
// (they are the points of communication with outer, potentially random world), where parsing functions
// are using helpers (like those that are tested here) that are being tested as well.

func FuzzCqlValueASCII(f *testing.F) {
	testCases := []string{"Hello World!", ""}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, data string) {
		in := CqlValue{
			Type:  &Option{ID: ASCIIID},
			Value: []byte(data),
		}
		ascii, err := in.AsASCII()
		if err != nil {
			// We skip tests with incorrect CqlValue.
			t.Skip()
		}
		out, err := CqlFromASCII(ascii)
		if err != nil {
			t.Errorf("cannot deserialize serialized data: %v", err)
		}
		if diff := cmp.Diff(in, out); diff != "" {
			t.Errorf("in: %v, out: %v", in, out)
		}
	})
}

func FuzzCqlValueBlob(f *testing.F) {
	testCases := [][]byte{[]byte("Hello World!"), []byte("")}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		in := CqlValue{
			Type:  &Option{ID: BlobID},
			Value: data,
		}
		x, err := in.AsBlob()
		if err != nil {
			// We skip tests with incorrect CqlValue.
			t.Skip()
		}
		out := CqlFromBlob(x)
		if diff := cmp.Diff(in, out); diff != "" {
			t.Errorf("in: %v, out: %v", in, out)
		}
	})
}

func FuzzCqlValueInt32(f *testing.F) {
	testCases := []int32{0x01020304, 0, 0xbeefed, -0xbeefed, math.MinInt32, math.MaxInt32}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, data int32) {
		in := CqlFromInt32(data)
		x, err := in.AsInt32()
		if err != nil {
			t.Errorf("cannot deserialize serialized data: %v", err)
		}
		out := CqlFromInt32(x)
		if diff := cmp.Diff(in, out); diff != "" {
			t.Errorf("in: %v, out: %v", in, out)
		}
	})
}

func FuzzCqlValueInt64(f *testing.F) {
	testCases := []int64{0x0102030405060708, 0, 0xdeadbeefcafe, -0xdeadbeefcafe, math.MinInt64, math.MaxInt64}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, data int64) {
		in := CqlFromInt64(data)
		x, err := in.AsInt64()
		if err != nil {
			t.Errorf("cannot deserialize serialized data: %v", err)
		}
		out := CqlFromInt64(x)
		if diff := cmp.Diff(in, out); diff != "" {
			t.Errorf("in: %v, out: %v", in, out)
		}
	})
}

func FuzzCqlValueInt16(f *testing.F) {
	testCases := []int16{0x0102, 0, 15, -15, math.MinInt16, math.MaxInt16}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, data int16) {
		in := CqlFromInt16(data)
		x, err := in.AsInt16()
		if err != nil {
			t.Errorf("cannot deserialize serialized data: %v", err)
		}
		out := CqlFromInt16(x)
		if diff := cmp.Diff(in, out); diff != "" {
			t.Errorf("in: %v, out: %v", in, out)
		}
	})
}

func FuzzCqlValueText(f *testing.F) {
	testCases := []string{"Hello World!", "", "½²=¼: Hello, 世界!"}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, data string) {
		in, err := CqlFromText(data)
		if err != nil {
			// We skip tests with incorrect CqlValue.
			t.Skip()
		}
		x, err := in.AsText()
		if err != nil {
			t.Errorf("cannot deserialize serialized data: %v", err)
		}
		out, err := CqlFromText(x)
		if err != nil {
			t.Errorf("second deserialization failed: %v", err)
		}
		if diff := cmp.Diff(in, out); diff != "" {
			t.Errorf("in: %v, out: %v", in, out)
		}
	})
}

func FuzzCqlValueIP(f *testing.F) {
	testCases := [][]byte{{1, 2, 3}, net.IP{127, 0, 0, 1}, net.IP{127, 0, 0, 1}.To16()}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		in, err := CqlFromIP(data)
		if err != nil {
			// We skip tests with incorrect CqlValue.
			t.Skip()
		}
		x, err := in.AsIP()
		if err != nil {
			t.Errorf("cannot deserialize serialized data: %v", err)
		}
		out, err := CqlFromIP(x)
		if err != nil {
			t.Errorf("second deserialization failed: %v", err)
		}
		if diff := cmp.Diff(in, out); diff != "" {
			t.Errorf("in: %v, out: %v", in, out)
		}
	})
}

func FuzzCqlValueFloat32(f *testing.F) {
	testCases := []float32{0, 1.5e30, -1.5e30, math.SmallestNonzeroFloat32, math.MaxFloat32}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, data float32) {
		in := CqlFromFloat32(data)
		x, err := in.AsFloat32()
		if err != nil {
			t.Errorf("cannot deserialize serialized data: %v", err)
		}
		out := CqlFromFloat32(x)
		if diff := cmp.Diff(in, out); diff != "" {
			t.Errorf("in: %v, out: %v", in, out)
		}
	})
}

func FuzzCqlValueFloat64(f *testing.F) {
	testCases := []float64{0, 1.5e120, -1.5e120, math.SmallestNonzeroFloat64, math.MaxFloat64}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, data float64) {
		in := CqlFromFloat64(data)
		x, err := in.AsFloat64()
		if err != nil {
			t.Errorf("cannot deserialize serialized data: %v", err)
		}
		out := CqlFromFloat64(x)
		if diff := cmp.Diff(in, out); diff != "" {
			t.Errorf("in: %v, out: %v", in, out)
		}
	})
}

func FuzzCqlValueStringSlice(f *testing.F) {
	testCases := [][4]string{{"1234567890", "rust", "cohle", "𝒽𝗲ɬł० ω𝒐ṙḹ𝖉"}, {"1234567890", "rust", "cohle", "Hello World!"}}
	for _, tc := range testCases {
		f.Add(tc[0], tc[1], tc[2], tc[3])
	}
	f.Fuzz(func(t *testing.T, a, b, c, d string) {
		in := []string{a, b, c, d}
		cv := CqlValue{
			Type: &Option{
				ID:   ListID,
				List: &ListOption{Element: Option{ID: VarcharID}},
			},
			Value: stringSliceToBytes(in),
		}
		out, err := cv.AsStringSlice()
		if err != nil {
			t.Errorf("cannot deserialize serialized data: %v", err)
		}
		if diff := cmp.Diff(in, out); diff != "" {
			t.Errorf("in: %v, out: %v", in, out)
		}
	})
}

func FuzzCqlValueStringMap(f *testing.F) {
	testCases := [][6]string{{"rust", "cohle", "hello", "world", "dead", "beef"}}
	for _, tc := range testCases {
		f.Add(tc[0], tc[1], tc[2], tc[3], tc[4], tc[5])
	}
	f.Fuzz(func(t *testing.T, a, b, c, d, e, f string) {
		in := map[string]string{a: b, c: d, e: f}
		cv := cqlStringMap(in, VarcharID, VarcharID)
		out, err := cv.AsStringMap()
		if err != nil {
			t.Errorf("cannot deserialize serialized data: %v", err)
		}
		if diff := cmp.Diff(in, out); diff != "" {
			t.Errorf("in: %v, out: %v", in, out)
		}
	})
}

func FuzzCqlValueDuration(f *testing.F) {
	f.Add(int32(1), int32(2), int64(3))
	f.Fuzz(func(t *testing.T, a, b int32, c int64) {
		in := Duration{
			Months:      a,
			Days:        b,
			Nanoseconds: c,
		}
		cv, err := CqlFromDuration(in)
		if err != nil {
			// Cannot serialize data, so we have no checks to do.
			// This happens if the sign of the values differs.
			return
		}
		out, err := cv.AsDuration()
		if err != nil {
			t.Errorf("cannot deserialize serialized data: %v", err)
		}
		if diff := cmp.Diff(in, out); diff != "" {
			t.Errorf("in: %v, out: %v", in, out)
		}
	})
}
