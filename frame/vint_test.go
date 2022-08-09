package frame

import (
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestDecodeVint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		data   []byte
		value  int64
		error  bool
		length int
	}{
		{
			name:  "empty",
			data:  []byte{},
			error: true,
		},
		{
			name:   "zero",
			data:   []byte{0x00},
			value:  0,
			length: 1,
		},
		{
			name:   "12",
			data:   []byte{0x18},
			value:  12,
			length: 1,
		},
		{
			name:   "123",
			data:   []byte{0x80, 0xf6},
			value:  123,
			length: 2,
		},
		{
			name:   "12345",
			data:   []byte{0xc0, 0x60, 0x72},
			value:  12345,
			length: 3,
		},
		{
			name:   "256000",
			data:   []byte{0xc7, 0xd0, 0x00},
			value:  256000,
			length: 3,
		},
		{
			name:   "1234567",
			data:   []byte{0xe0, 0x25, 0xad, 0x0e},
			value:  1234567,
			length: 4,
		},
		{
			name:   "1234567890",
			data:   []byte{0xf0, 0x93, 0x2c, 0x05, 0xa4},
			value:  1234567890,
			length: 5,
		},
		{
			name:   "123456789012",
			data:   []byte{0xf8, 0x39, 0x7d, 0x32, 0x34, 0x28},
			value:  123456789012,
			length: 6,
		},
		{
			name:   "12345678901234",
			data:   []byte{0xfc, 0x16, 0x74, 0xe7, 0x9c, 0x5f, 0xe4},
			value:  12345678901234,
			length: 7,
		},
		{
			name:   "1234567890123456",
			data:   []byte{0xfe, 0x08, 0xc5, 0xaa, 0x79, 0x15, 0x75, 0x80},
			value:  1234567890123456,
			length: 8,
		},
		{
			name:   "-123456789",
			data:   []byte{0xee, 0xb7, 0x9a, 0x29},
			value:  -123456789,
			length: 4,
		},
		{
			name:   "MaxInt64",
			data:   []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe},
			value:  math.MaxInt64,
			length: 9,
		},
		{
			name:   "MinInt64",
			data:   []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			value:  math.MinInt64,
			length: 9,
		},
		{
			name:   "more data",
			data:   []byte{0xc7, 0xd0, 0x00, 0x01, 0x02, 0x03},
			value:  256000,
			length: 3,
		},
		{
			name:  "short read",
			data:  []byte{0xff, 0xff, 0xff, 0xff},
			error: true,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			value, length, err := decodeVInt(tc.data)
			if tc.error {
				if err == nil {
					t.Fatal("expected error, but got nil")
				}
				return
			}
			if diff := cmp.Diff(value, tc.value); diff != "" {
				t.Fatalf("value differs\n%s", diff)
			}
			if diff := cmp.Diff(length, tc.length); diff != "" {
				t.Fatalf("length differs\n%s", diff)
			}
		})
	}
}

func TestAppendVint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		data  []byte
		value int64
	}{
		{
			name:  "zero",
			data:  []byte{0x00},
			value: 0,
		},
		{
			name:  "12",
			data:  []byte{0x18},
			value: 12,
		},
		{
			name:  "123",
			data:  []byte{0x80, 0xf6},
			value: 123,
		},
		{
			name:  "12345",
			data:  []byte{0xc0, 0x60, 0x72},
			value: 12345,
		},
		{
			name:  "256000",
			data:  []byte{0xc7, 0xd0, 0x00},
			value: 256000,
		},
		{
			name:  "1234567",
			data:  []byte{0xe0, 0x25, 0xad, 0x0e},
			value: 1234567,
		},
		{
			name:  "1234567890",
			data:  []byte{0xf0, 0x93, 0x2c, 0x05, 0xa4},
			value: 1234567890,
		},
		{
			name:  "123456789012",
			data:  []byte{0xf8, 0x39, 0x7d, 0x32, 0x34, 0x28},
			value: 123456789012,
		},
		{
			name:  "12345678901234",
			data:  []byte{0xfc, 0x16, 0x74, 0xe7, 0x9c, 0x5f, 0xe4},
			value: 12345678901234,
		},
		{
			name:  "1234567890123456",
			data:  []byte{0xfe, 0x08, 0xc5, 0xaa, 0x79, 0x15, 0x75, 0x80},
			value: 1234567890123456,
		},
		{
			name:  "-123456789",
			data:  []byte{0xee, 0xb7, 0x9a, 0x29},
			value: -123456789,
		},
		{
			name:  "MaxInt64",
			data:  []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe},
			value: math.MaxInt64,
		},
		{
			name:  "MinInt64",
			data:  []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			value: math.MinInt64,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := []byte{0x01, 0x02}
			data = appendVInt(data, tc.value)

			expected := []byte{0x01, 0x02}
			expected = append(expected, tc.data...)

			if diff := cmp.Diff(data, expected); diff != "" {
				t.Fatalf("data differs\n%s", diff)
			}
		})
	}
}
