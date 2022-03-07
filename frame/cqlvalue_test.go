package frame

import (
	"math"
	"testing"
)

func TestCqlValueAsASCII(t *testing.T) {
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
				Value: Bytes{'a', 'b', 'c', 'd'},
			},
			valid:    true,
			expected: "abcd",
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
		{name: "wrong type",
			content: CqlValue{
				Type: &Option{ID: BlobID},
			},
			valid: false,
		},
		{name: "non-ascii characters",
			content: CqlValue{
				Type:  &Option{ID: ASCIIID},
				Value: Bytes{133, 213},
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

func TestCqlValueAsInt(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  CqlValue
		valid    bool
		expected int32
	}{
		{
			name: "byte order",
			content: CqlValue{
				Type:  &Option{ID: IntID},
				Value: Bytes{0x01, 0x02, 0x03, 0x04},
			},
			valid:    true,
			expected: 0x01020304,
		},
		{
			name: "zero",
			content: CqlValue{
				Type:  &Option{ID: IntID},
				Value: Bytes{0, 0, 0, 0},
			},
			valid:    true,
			expected: 0,
		},
		{
			name: "wrong type",
			content: CqlValue{
				Type: &Option{ID: BlobID},
			},
			valid: false,
		},
		{
			name: "int32.min",
			content: CqlValue{
				Type:  &Option{ID: IntID},
				Value: Bytes{0x80, 0x0, 0x0, 0x0},
			},
			valid:    true,
			expected: math.MinInt32,
		},
		{
			name: "int32.max",
			content: CqlValue{
				Type:  &Option{ID: IntID},
				Value: Bytes{0x7f, 0xff, 0xff, 0xff},
			},
			valid:    true,
			expected: math.MaxInt32,
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := tc.content.AsInt()
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
