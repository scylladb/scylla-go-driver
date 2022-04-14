package frame

import (
	"math"
	"net"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCqlValueAsASCII(t *testing.T) { // nolint:dupl // Tests are different.
	t.Parallel()
	testCases := []struct {
		name     string
		content  CqlValue
		valid    bool
		expected string
	}{
		{
			name: "byte order",
			content: CqlValue{
				Type:  &Option{ID: ASCIIID},
				Value: Bytes("Hello World!"),
			},
			valid:    true,
			expected: "Hello World!",
		},
		{
			name: "empty string",
			content: CqlValue{
				Type:  &Option{ID: ASCIIID},
				Value: Bytes{},
			},
			valid:    true,
			expected: "",
		},
		{
			name: "wrong type",
			content: CqlValue{
				Type: &Option{ID: BlobID},
			},
			valid: false,
		},
		{
			name: "non-ascii characters",
			content: CqlValue{
				Type:  &Option{ID: ASCIIID},
				Value: Bytes("½²=¼"),
			},
			valid: false,
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := tc.content.AsASCII()
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}

			if v != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, v)
			}
		})
	}
}

func TestCqlValueAsBlob(t *testing.T) { // nolint:dupl // Tests are different.
	t.Parallel()
	testCases := []struct {
		name     string
		content  CqlValue
		valid    bool
		expected []byte
	}{
		{
			name: "byte order",
			content: CqlValue{
				Type:  &Option{ID: BlobID},
				Value: Bytes("Hello World!"),
			},
			valid:    true,
			expected: []byte("Hello World!"),
		},
		{
			name: "empty blob",
			content: CqlValue{
				Type:  &Option{ID: BlobID},
				Value: Bytes{},
			},
			valid:    true,
			expected: []byte{},
		},
		{
			name: "wrong type",
			content: CqlValue{
				Type: &Option{ID: VarcharID},
			},
			valid: false,
		},
		{
			name: "all byte values",
			content: CqlValue{
				Type: &Option{ID: BlobID},
				Value: func() Bytes {
					v := make(Bytes, 256)
					for i := 0; i < 256; i++ {
						v[i] = byte(i)
					}
					return v
				}(),
			},
			valid: true,
			expected: func() []byte {
				v := make([]byte, 256)
				for i := 0; i < 256; i++ {
					v[i] = byte(i)
				}
				return v
			}(),
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := tc.content.AsBlob()
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}

			if diff := cmp.Diff(v, tc.expected); diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func TestCqlValueAsBoolean(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  CqlValue
		valid    bool
		expected bool
	}{
		{
			name:     "true",
			content:  CqlFromBoolean(true),
			valid:    true,
			expected: true,
		},
		{
			name:     "false",
			content:  CqlFromBoolean(false),
			valid:    true,
			expected: false,
		},
		{
			name:    "invalid type",
			content: CqlFromInt8(int8(21)),
			valid:   false,
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := tc.content.AsBoolean()
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}

			if diff := cmp.Diff(v, tc.expected); diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func TestCqlValueAsInt32(t *testing.T) { //nolint:dupl // Tests are different.
	t.Parallel()
	testCases := []struct {
		name     string
		content  CqlValue
		valid    bool
		expected int32
	}{
		{
			name:     "byte order",
			content:  CqlFromInt32(0x01020304),
			valid:    true,
			expected: 0x01020304,
		},
		{
			name:     "zero",
			content:  CqlFromInt32(0),
			valid:    true,
			expected: 0,
		},
		{
			name:     "positive value",
			content:  CqlFromInt32(0xbeefed),
			valid:    true,
			expected: 0xbeefed,
		},
		{
			name:     "negative value",
			content:  CqlFromInt32(-0xbeefed),
			valid:    true,
			expected: -0xbeefed,
		},
		{
			name: "wrong type",
			content: CqlValue{
				Type: &Option{ID: BlobID},
			},
			valid: false,
		},
		{
			name:     "int32.min",
			content:  CqlFromInt32(math.MinInt32),
			valid:    true,
			expected: math.MinInt32,
		},
		{
			name:     "int32.max",
			content:  CqlFromInt32(math.MaxInt32),
			valid:    true,
			expected: math.MaxInt32,
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := tc.content.AsInt32()
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}

			if v != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, v)
			}
		})
	}
}

func TestCqlValueAsInt64(t *testing.T) { //nolint:dupl // Tests are different.
	t.Parallel()
	testCases := []struct {
		name     string
		content  CqlValue
		valid    bool
		expected int64
	}{
		{
			name:     "byte order",
			content:  CqlFromInt64(0x0102030405060708),
			valid:    true,
			expected: 0x0102030405060708,
		},
		{
			name:     "zero",
			content:  CqlFromInt64(0),
			valid:    true,
			expected: 0,
		},
		{
			name:     "positive value",
			content:  CqlFromInt64(0xdeadbeefcafe),
			valid:    true,
			expected: 0xdeadbeefcafe,
		},
		{
			name:     "negative value",
			content:  CqlFromInt64(-0xdeadbeefcafe),
			valid:    true,
			expected: -0xdeadbeefcafe,
		},
		{
			name: "wrong type",
			content: CqlValue{
				Type: &Option{ID: BlobID},
			},
			valid: false,
		},
		{
			name:     "int64.min",
			content:  CqlFromInt64(math.MinInt64),
			valid:    true,
			expected: math.MinInt64,
		},
		{
			name:     "int64.max",
			content:  CqlFromInt64(math.MaxInt64),
			valid:    true,
			expected: math.MaxInt64,
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := tc.content.AsInt64()
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}

			if v != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, v)
			}
		})
	}
}

func TestCqlValueAsInt16(t *testing.T) { //nolint:dupl // Tests are different.
	t.Parallel()
	testCases := []struct {
		name     string
		content  CqlValue
		valid    bool
		expected int16
	}{
		{
			name:     "byte order",
			content:  CqlFromInt16(0x0102),
			valid:    true,
			expected: 0x0102,
		},
		{
			name:     "zero",
			content:  CqlFromInt16(0),
			valid:    true,
			expected: 0,
		},
		{
			name:     "positive value",
			content:  CqlFromInt16(15),
			valid:    true,
			expected: 15,
		},
		{
			name:     "negative value",
			content:  CqlFromInt16(-15),
			valid:    true,
			expected: -15,
		},
		{
			name: "wrong type",
			content: CqlValue{
				Type: &Option{ID: BlobID},
			},
			valid: false,
		},
		{
			name:     "int16.min",
			content:  CqlFromInt16(math.MinInt16),
			valid:    true,
			expected: math.MinInt16,
		},
		{
			name:     "int16.max",
			content:  CqlFromInt16(math.MaxInt16),
			valid:    true,
			expected: math.MaxInt16,
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := tc.content.AsInt16()
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}

			if v != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, v)
			}
		})
	}
}

func TestCqlValueAsInt8(t *testing.T) {
	t.Parallel()

	for i := math.MinInt8; i <= math.MaxInt8; i++ {
		c := CqlFromInt8(int8(i))
		v, err := c.AsInt8()
		if err != nil {
			t.Fatal(err)
		}

		if v != int8(i) {
			t.Fatalf("expected %d, got %d", i, v)
		}
	}

	c := CqlValue{
		Type: &Option{ID: BlobID},
	}

	_, err := c.AsInt8()
	if err == nil {
		t.Fatalf("%v shouldn't be able to be interpreted as int8", c)
	}
}

func TestCqlValueAsText(t *testing.T) { // nolint:dupl // Tests are different.
	t.Parallel()
	testCases := []struct {
		name     string
		content  CqlValue
		valid    bool
		expected string
	}{
		{
			name: "byte order",
			content: CqlValue{
				Type:  &Option{ID: VarcharID},
				Value: Bytes("Hello World!"),
			},
			valid:    true,
			expected: "Hello World!",
		},
		{
			name: "empty string",
			content: CqlValue{
				Type:  &Option{ID: VarcharID},
				Value: Bytes{},
			},
			valid:    true,
			expected: "",
		},
		{
			name: "wrong type",
			content: CqlValue{
				Type: &Option{ID: BlobID},
			},
			valid: false,
		},
		{
			name: "non-ascii utf-8 characters",
			content: CqlValue{
				Type:  &Option{ID: VarcharID},
				Value: Bytes("½²=¼: Hello, 世界!"),
			},
			valid:    true,
			expected: "½²=¼: Hello, 世界!",
		},
		{
			name: "non utf-8 characters",
			content: CqlValue{
				Type:  &Option{ID: VarcharID},
				Value: Bytes{0xff, 0xfe, 0xfd},
			},
			valid: false,
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := tc.content.AsText()
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}

			if v != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, v)
			}
		})
	}
}

func TestCqlValueAsIP(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  CqlValue
		valid    bool
		expected net.IP
	}{
		{
			name: "wrong length",
			content: CqlValue{
				Type:  &Option{ID: InetID},
				Value: Bytes{1, 2, 3},
			},
			valid: false,
		},
		{
			name: "wrong type",
			content: CqlValue{
				Type: &Option{ID: BlobID},
			},
			valid: false,
		},
		{
			name: "valid v4",
			content: CqlValue{
				Type:  &Option{ID: InetID},
				Value: Bytes(net.IP{127, 0, 0, 1}),
			},
			valid:    true,
			expected: net.IP{127, 0, 0, 1},
		},
		{
			name: "valid v6",
			content: CqlValue{
				Type:  &Option{ID: InetID},
				Value: Bytes(net.IP{127, 0, 0, 1}.To16()),
			},
			valid:    true,
			expected: net.IP{127, 0, 0, 1}.To16(),
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := tc.content.AsIP()
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}
			if diff := cmp.Diff(v, tc.expected); diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}
func TestCqlValueAsFloat32(t *testing.T) { //nolint:dupl // Tests are different.
	t.Parallel()
	testCases := []struct {
		name     string
		content  CqlValue
		valid    bool
		expected float32
	}{
		{
			name:     "zero",
			content:  CqlFromFloat32(0),
			valid:    true,
			expected: 0,
		},
		{
			name:     "positive value",
			content:  CqlFromFloat32(1.5e30),
			valid:    true,
			expected: 1.5e30,
		},
		{
			name:     "negative value",
			content:  CqlFromFloat32(-1.5e30),
			valid:    true,
			expected: -1.5e30,
		},
		{
			name: "wrong type",
			content: CqlValue{
				Type: &Option{ID: BlobID},
			},
			valid: false,
		},
		{
			name:     "smallest nonzero float32",
			content:  CqlFromFloat32(math.SmallestNonzeroFloat32),
			valid:    true,
			expected: math.SmallestNonzeroFloat32,
		},
		{
			name:     "float32.max",
			content:  CqlFromFloat32(math.MaxFloat32),
			valid:    true,
			expected: math.MaxFloat32,
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := tc.content.AsFloat32()
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}

			if v != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, v)
			}
		})
	}
}

func TestCqlValueAsFloat64(t *testing.T) { //nolint:dupl // Tests are different.
	t.Parallel()
	testCases := []struct {
		name     string
		content  CqlValue
		valid    bool
		expected float64
	}{
		{
			name:     "zero",
			content:  CqlFromFloat64(0),
			valid:    true,
			expected: 0,
		},
		{
			name:     "positive value",
			content:  CqlFromFloat64(1.5e120),
			valid:    true,
			expected: 1.5e120,
		},
		{
			name:     "negative value",
			content:  CqlFromFloat64(-1.5e120),
			valid:    true,
			expected: -1.5e120,
		},
		{
			name: "wrong type",
			content: CqlValue{
				Type: &Option{ID: BlobID},
			},
			valid: false,
		},
		{
			name:     "smallest nonzero float64",
			content:  CqlFromFloat64(math.SmallestNonzeroFloat64),
			valid:    true,
			expected: math.SmallestNonzeroFloat64,
		},
		{
			name:     "float32.max",
			content:  CqlFromFloat64(math.MaxFloat64),
			valid:    true,
			expected: math.MaxFloat64,
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := tc.content.AsFloat64()
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}

			if v != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, v)
			}
		})
	}
}

func stringSliceToBytes(s []string) Bytes {
	var b Buffer
	b.WriteInt(int32(len(s)))
	for _, v := range s {
		b.WriteLongString(v)
	}
	return b.Bytes()
}

func TestCqlValueStringSlice(t *testing.T) { // nolint:dupl // Tests are different.
	t.Parallel()

	testCases := []struct {
		name     string
		content  CqlValue
		expected []string
		valid    bool
	}{
		{
			name: "set<text>",
			content: CqlValue{
				Type: &Option{
					ID: SetID,
					Set: &SetOption{
						Element: Option{ID: VarcharID},
					},
				},
				Value: stringSliceToBytes([]string{"1234567890", "rust", "cohle", "𝒽𝗲ɬł० ω𝒐ṙḹ𝖉"}),
			},
			expected: []string{"1234567890", "rust", "cohle", "𝒽𝗲ɬł० ω𝒐ṙḹ𝖉"},
			valid:    true,
		},
		{
			name: "list<text>",
			content: CqlValue{
				Type: &Option{
					ID: ListID,
					List: &ListOption{
						Element: Option{ID: VarcharID},
					},
				},
				Value: stringSliceToBytes([]string{"1234567890", "rust", "cohle", "𝒽𝗲ɬł० ω𝒐ṙḹ𝖉"}),
			},
			expected: []string{"1234567890", "rust", "cohle", "𝒽𝗲ɬł० ω𝒐ṙḹ𝖉"},
			valid:    true,
		},
		{
			name: "list<ascii>",
			content: CqlValue{
				Type: &Option{
					ID: ListID,
					List: &ListOption{
						Element: Option{ID: ASCIIID},
					},
				},
				Value: stringSliceToBytes([]string{"1234567890", "rust", "cohle", "Hello World!"}),
			},
			expected: []string{"1234567890", "rust", "cohle", "Hello World!"},
			valid:    true,
		},
		{
			name: "list<ascii>",
			content: CqlValue{
				Type: &Option{
					ID: ListID,
					List: &ListOption{
						Element: Option{ID: ASCIIID},
					},
				},
				Value: stringSliceToBytes([]string{"1234567890", "rust", "cohle", "Hello World!"}),
			},
			expected: []string{"1234567890", "rust", "cohle", "Hello World!"},
			valid:    true,
		},
		{
			name:    "non-slice type",
			content: CqlValue{Type: &Option{ID: MapID}},
		},
		{
			name: "wrong set element type",
			content: CqlValue{
				Type: &Option{
					ID: SetID,
					Set: &SetOption{
						Element: Option{ID: IntID},
					},
				},
			},
		},
		{
			name: "wrong list element type",
			content: CqlValue{
				Type: &Option{
					ID: ListID,
					List: &ListOption{
						Element: Option{ID: IntID},
					},
				},
			},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res, err := tc.content.AsStringSlice()
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}
			if diff := cmp.Diff(res, tc.expected); diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func cqlStringMap(m map[string]string, keyTypeID, valueTypeID OptionID) CqlValue {
	var b Buffer
	b.WriteInt(int32(len(m)))
	for k, v := range m {
		b.WriteLongString(k)
		b.WriteLongString(v)
	}

	return CqlValue{
		Type: &Option{
			ID:  MapID,
			Map: &MapOption{Key: Option{ID: keyTypeID}, Value: Option{ID: valueTypeID}}},
		Value: b.Bytes(),
	}
}

func TestCqlValueStringMap(t *testing.T) {
	t.Parallel()

	sampleMap := map[string]string{
		"rust":  "cohle",
		"hello": "world",
		"dead":  "beef",
		"":      "a",
		"b":     "",
	}

	testCases := []struct {
		name     string
		content  CqlValue
		expected map[string]string
		valid    bool
	}{
		{
			name:     "map<text,text>",
			content:  cqlStringMap(sampleMap, VarcharID, VarcharID),
			expected: sampleMap,
			valid:    true,
		},
		{
			name:     "map<ascii,text>",
			content:  cqlStringMap(sampleMap, ASCIIID, VarcharID),
			expected: sampleMap,
			valid:    true,
		},
		{
			name:     "map<text,ascii>",
			content:  cqlStringMap(sampleMap, ASCIIID, VarcharID),
			expected: sampleMap,
			valid:    true,
		},
		{
			name:     "map<ascii,ascii>",
			content:  cqlStringMap(sampleMap, ASCIIID, ASCIIID),
			expected: sampleMap,
			valid:    true,
		},
		{
			name:     "empty map",
			content:  cqlStringMap(map[string]string{}, VarcharID, VarcharID),
			expected: map[string]string{},
			valid:    true,
		},
		{
			name:    "nonstring value",
			content: cqlStringMap(map[string]string{}, VarcharID, IntID),
			valid:   false,
		},
		{
			name:    "non-map",
			content: CqlValue{Type: &Option{ID: IntID}},
			valid:   false,
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res, err := tc.content.AsStringMap()
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}
			if diff := cmp.Diff(res, tc.expected); diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

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
	f.Fuzz(func(t *testing.T, data string) { // nolint:thelper // This is not a helper function.
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
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
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
	f.Fuzz(func(t *testing.T, data int32) { // nolint:thelper // This is not a helper function.
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
	f.Fuzz(func(t *testing.T, data int64) { // nolint:thelper // This is not a helper function.
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
	f.Fuzz(func(t *testing.T, data int16) { // nolint:thelper // This is not a helper function.
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
	f.Fuzz(func(t *testing.T, data string) { // nolint:thelper // This is not a helper function.
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
	f.Fuzz(func(t *testing.T, data []byte) { // nolint:thelper // This is not a helper function.
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
	f.Fuzz(func(t *testing.T, data float32) { // nolint:thelper // This is not a helper function.
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
	f.Fuzz(func(t *testing.T, data float64) { // nolint:thelper // This is not a helper function.
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
	f.Fuzz(func(t *testing.T, a, b, c, d string) { // nolint:thelper // This is not a helper function.
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
	f.Fuzz(func(t *testing.T, a, b, c, d, e, f string) { // nolint:thelper // This is not a helper function.
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
