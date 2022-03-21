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

func TestCqlValueTextSet(t *testing.T) { // nolint:dupl // Tests are different.
	t.Parallel()

	testCases := []struct {
		name    string
		content []string
		valid   bool
	}{
		{
			name:    "empty list",
			content: []string{},
			valid:   true,
		},
		{
			name:    "valid list",
			content: []string{"1234567890", "rust", "cohle", "𝒽𝗲ɬł० ω𝒐ṙḹ𝖉"},
			valid:   true,
		},
		{
			name:    "invalid utf-8 in list",
			content: []string{"\xff"},
			valid:   false,
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			serialized, err := CqlFromTextSet(tc.content)
			if tc.valid && err != nil {
				t.Fatal(err)
			}
			if !tc.valid {
				if err == nil {
					t.Fatalf("serializing %v as TextSet should result in error", tc.content)
				}
				return
			}

			deserialized, err := serialized.AsTextSet()
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(deserialized, tc.content); diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}
