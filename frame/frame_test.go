package frame

import (
	"bytes"
	"fmt"
	"testing"
)

//------------------------------- WRITE TESTS ---------------------------------

func TestWriteByte(t *testing.T) {
	var cases = []struct {
		name     string
		nr       Byte
		expected []byte
	}{
		{"min byte", 0, []byte{0x0}},
		{"min positive byte", 1, []byte{0x01}},
		{"random big byte", 173, []byte{0xad}},
		{"max byte", 255, []byte{0xff}},
	}

	var buf bytes.Buffer
	for _, tc := range cases {

		t.Run(fmt.Sprintf("Bytes writing test %s", tc.name), func(t *testing.T) {
			WriteByte(tc.nr, &buf)

			if !bytes.Equal(buf.Bytes(), tc.expected) {
				t.Fatal("Failure while writing byte to a buffer.")
			}
		})

		buf.Reset()
	}
}

func TestWriteShort(t *testing.T) {
	var cases = []struct {
		name     string
		nr       Short
		expected []byte
	}{
		{"min short", 0, []byte{0x0, 0x0}},
		{"max byte", 255, []byte{0x0, 0xff}},
		{"min non byte", 256, []byte{0x01, 0x00}},
		{"random big short", 7919, []byte{0x1e, 0xef}},
		{"max short", 65535, []byte{0xff, 0xff}},
	}

	var buf bytes.Buffer
	for _, tc := range cases {

		t.Run(fmt.Sprintf("Short writing test %s", tc.name), func(t *testing.T) {
			WriteShort(tc.nr, &buf)

			if !bytes.Equal(buf.Bytes(), tc.expected) {
				t.Fatal("Failure while writing unsigned short to a buffer.")
			}
		})

		buf.Reset()
	}
}

func TestWriteInt(t *testing.T) {
	var cases = []struct {
		name     string
		nr       Int
		expected []byte
	}{
		{"min integer", -2147483648, []byte{0x80, 0x0, 0x0, 0x0}},
		{"zero", 0, []byte{0x0, 0x0, 0x0, 0x0}},
		{"min positive integer", 1, []byte{0x0, 0x0, 0x0, 0x01}},
		{"random short", 9452, []byte{0x0, 0x0, 0x24, 0xec}},
		{"random 3 byte numer", 123335, []byte{0x0, 0x01, 0xe1, 0xc7}},
		{"max integer", 2147483647, []byte{0x7f, 0xff, 0xff, 0xff}},
	}

	var buf bytes.Buffer
	for _, tc := range cases {

		t.Run(fmt.Sprintf("Integer writing test %s", tc.name), func(t *testing.T) {
			WriteInt(tc.nr, &buf)

			if !bytes.Equal(buf.Bytes(), tc.expected) {
				t.Fatal("Failure while writing integer to a buffer.")
			}
		})

		buf.Reset()
	}
}

func TestWriteString(t *testing.T) {
	var cases = []struct {
		name     string
		content  string
		expected []byte
	}{
		{"one char", "a", []byte{0x00, 0x01, 0x61}},
		{"normal word", "golang", []byte{0x00, 0x06, 0x67, 0x6f, 0x6c, 0x61, 0x6e, 0x67}},
		{"UTF-8 characters", "πœę©ß", []byte{0x00, 0x0a, 0xcf, 0x80, 0xc5, 0x93, 0xc4, 0x99, 0xc2, 0xa9, 0xc3, 0x9f}},
		{"empty string", "", []byte{0x00, 0x00}},
	}

	var buf bytes.Buffer
	for _, tc := range cases {

		t.Run(fmt.Sprintf("String writing test %s", tc.name), func(t *testing.T) {
			WriteString(tc.content, &buf)

			if !bytes.Equal(buf.Bytes(), tc.expected) {
				t.Fatal("Failure while writing string to a buffer.")
			}
		})

		buf.Reset()
	}
}

func TestWriteStringList(t *testing.T) {
	var cases = []struct {
		name     string
		content  StringList
		expected []byte
	}{
		{"one string", StringList{"a"}, []byte{0x00, 0x01, 0x00, 0x01, 0x61}},
		{"two strings", StringList{"a", "b"}, []byte{0x00, 0x02, 0x00, 0x01, 0x61, 0x00, 0x01, 0x62}},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("StringList writing test %s", tc.name), func(t *testing.T) {
			WriteStringList(tc.content, &buf)

			if !bytes.Equal(buf.Bytes(), tc.expected) {
				t.Fatal("Failure while writing StringList.")
			}
		})

		buf.Reset()
	}
}

func TestWriteStringMultiMap(t *testing.T) {
	var cases = []struct {
		name     string
		content  StringMultiMap
		expected []byte
	}{
		{"Smoke test", StringMultiMap{"a": {"a"}}, []byte{0x00, 0x01, 0x00, 0x01, 0x61, 0x00, 0x01, 0x00, 0x01, 0x61}},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("StringMultiMap writing test %s", tc.name), func(t *testing.T) {
			WriteStringMultiMap(tc.content, &buf)

			if !bytes.Equal(buf.Bytes(), tc.expected) {
				t.Fatal("Failure while writing StringMultiMap.")
			}
		})

		buf.Reset()
	}
}

func TestWriteHeader(t *testing.T) {
	var cases = []struct {
		name     string
		content  Header
		expected []byte
	}{
		{"plain supported",
			Header{
				Version:  CQLv4,
				Flags:    0,
				StreamID: 0,
				Opcode:   OpSupported,
				Length:   0,
			},
			[]byte{0x84, 0x0, 0x0, 0x0, 0x06, 0x0, 0x0, 0x0, 0x0},
		},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("StringMultiMap reading test %s", tc.name), func(t *testing.T) {
			tc.content.Write(&buf)

			if !bytes.Equal(buf.Bytes(), tc.expected) {
				t.Fatal("Failure while reading StringMultiMap.")
			}
		})

		buf.Reset()
	}
}

//------------------------------- READ TESTS ----------------------------------

func TestReadByte(t *testing.T) {
	var cases = []struct {
		name     string
		nr       []byte
		expected Byte
	}{
		{"min byte", []byte{0x00}, 0},
		{"random small byte", []byte{0x16}, 22},
		{"random large byte", []byte{0x7d}, 125},
		{"max byte", []byte{0xff}, 255},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Byte reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.nr)
			out := ReadByte(&buf)

			if out != tc.expected {
				t.Fatal("Failure while reading Byte.")
			}
		})

		buf.Reset()
	}
}

func TestReadShort(t *testing.T) {
	var cases = []struct {
		name     string
		nr       []byte
		expected Short
	}{
		{"min short", []byte{0x00, 0x00}, 0},
		{"random small short", []byte{0x00, 0xf5}, 245},
		{"random large short", []byte{0xa7, 0xf3}, 42995},
		{"max short", []byte{0xff, 0xff}, 65535},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Short reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.nr)
			out := ReadShort(&buf)

			if out != tc.expected {
				t.Fatal("Failure while reading Short.")
			}
		})

		buf.Reset()
	}
}

func TestReadInt(t *testing.T) {
	var cases = []struct {
		name     string
		nr       []byte
		expected Int
	}{
		{"min integer", []byte{0x80, 0x0, 0x0, 0x0}, -2147483648},
		{"zero", []byte{0x0, 0x0, 0x0, 0x0}, 0},
		{"min positive integer", []byte{0x0, 0x0, 0x0, 0x01}, 1},
		{"random short", []byte{0x0, 0x0, 0x24, 0xec}, 9452},
		{"random 3 byte numer", []byte{0x0, 0x01, 0xe1, 0xc7}, 123335},
		{"max integer", []byte{0x7f, 0xff, 0xff, 0xff}, 2147483647},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("Integer reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.nr)
			out := ReadInt(&buf)

			if out != tc.expected {
				t.Fatal("Failure while reading Integer.")
			}
		})

		buf.Reset()
	}
}

func TestReadString(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected string
	}{
		{"one char", []byte{0x00, 0x01, 0x61}, "a"},
		{"normal word", []byte{0x00, 0x06, 0x67, 0x6f, 0x6c, 0x61, 0x6e, 0x67}, "golang"},
		{"UTF-8 characters", []byte{0x00, 0x0a, 0xcf, 0x80, 0xc5, 0x93, 0xc4, 0x99, 0xc2, 0xa9, 0xc3, 0x9f}, "πœę©ß"},
		{"empty string", []byte{0x00, 0x00}, ""},
	}

	var buf bytes.Buffer
	for _, tc := range cases {

		t.Run(fmt.Sprintf("String reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)
			out := ReadString(&buf)

			if out != tc.expected {
				t.Fatal("Failure while writing reading String.")
			}
		})

		buf.Reset()
	}
}

// equalStringList checks equality between two StringLists,
// by writing function on our own we avoid reflect.DeepEqual function.
func equalStringList(a, b StringList) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func TestReadStringList(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected StringList
	}{
		{"one string", []byte{0x00, 0x01, 0x00, 0x01, 0x61}, StringList{"a"}},
		{"two strings", []byte{0x00, 0x02, 0x00, 0x01, 0x61, 0x00, 0x01, 0x62}, StringList{"a", "b"}},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("StringList reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)
			out := ReadStringList(&buf)

			if !equalStringList(out, tc.expected) {
				t.Fatal("Failure while reading StringList.")
			}
		})

		buf.Reset()
	}
}

// equalStringMultiMap checks equality between two StringMultiMaps,
// by writing function on our own we avoid reflect.DeepEqual function.
func equalStringMultiMap(a, b StringMultiMap) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if !equalStringList(v, b[i]) {
			return false
		}
	}
	return true
}

func TestReadStringMultiMap(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected StringMultiMap
	}{
		{"Smoke test", []byte{0x00, 0x01, 0x00, 0x01, 0x61, 0x00, 0x01, 0x00, 0x01, 0x61}, StringMultiMap{"a": {"a"}}},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("StringMultiMap reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)
			out := ReadStringMultiMap(&buf)

			if !equalStringMultiMap(out, tc.expected) {
				t.Fatal("Failure while reading StringMultiMap.")
			}
		})

		buf.Reset()
	}
}

func TestReadHeader(t *testing.T) {
	var cases = []struct {
		name     string
		content  []byte
		expected Header
	}{
		{"plain supported",
			[]byte{0x84, 0x0, 0x0, 0x0, 0x06, 0x0, 0x0, 0x0, 0x0},
			Header{
				Version:  CQLv4,
				Flags:    0,
				StreamID: 0,
				Opcode:   OpSupported,
				Length:   0,
			},
		},
	}

	var buf bytes.Buffer
	for _, tc := range cases {
		t.Run(fmt.Sprintf("StringMultiMap reading test %s", tc.name), func(t *testing.T) {
			buf.Write(tc.content)
			out := ReadHeader(&buf)

			if out != tc.expected {
				t.Fatal("Failure while reading StringMultiMap.")
			}
		})

		buf.Reset()
	}
}

//------------------------------- BENCHMARKS ----------------------------------

// result ensures that compiler won't skip operations
// during optimization of the benchmark functions.
// That's the reason why functions assign value to it.
var result Int

// fullBuffer creates and returns buffer of length n
// that is filled with bytes of consecutive values.
func fullBuffer(n int) *bytes.Buffer {
	buf := &bytes.Buffer{}
	for i := 0; i <= n; i++ {
		buf.WriteByte(byte(i % 255))
	}
	return buf
}

// ReadIntWithSlice reads and returns Int by reading
// all four bytes at once to allocated byte slice.
func ReadIntWithSlice(b *bytes.Buffer) Int {
	tmp := make([]byte, 4)
	_, _ = b.Read(tmp)
	return Int(tmp[0])<<24 |
		Int(tmp[1])<<16 |
		Int(tmp[2])<<8 |
		Int(tmp[3])
}

// ReadIntWithSlice reads and returns Int by reading
// all four bytes at once to allocated byte slice.
func ReadIntWithSliceNoAlloc(b *bytes.Buffer) Int {
	tmp := []byte{0, 0, 0, 0}
	_, _ = b.Read(tmp)
	return Int(tmp[0])<<24 |
		Int(tmp[1])<<16 |
		Int(tmp[2])<<8 |
		Int(tmp[3])
}

// ReadShortWithSlice reads and returns Short by reading
// all two bytes at once to allocated byte slice.
func ReadShortWithSlice(b *bytes.Buffer) Short {
	tmp := make([]byte, 2)
	_, _ = b.Read(tmp)
	return Short(tmp[0])<<8 | Short(tmp[1])
}

// ReadShortWithSlice reads and returns Short by reading
// all two bytes at once to allocated byte slice.
func ReadShortWithSliceNoAlloc(b *bytes.Buffer) Short {
	tmp := []byte{0, 0}
	_, _ = b.Read(tmp)
	return Short(tmp[0])<<8 | Short(tmp[1])
}

// ReadIntWithByte reads and returns Int by reading two Shorts.
func ReadIntWithByte(b *bytes.Buffer) Int {
	return Int(ReadByte(b))<<24 | Int(ReadByte(b))<<16 | Int(ReadByte(b))<<8 | Int(ReadByte(b))
}

// ReadIntWithShort reads and returns Int by reading two Shorts.
func ReadIntWithShort(b *bytes.Buffer) Int {
	return Int(ReadShortWithByte(b))<<16 | Int(ReadShortWithByte(b))
}

// ReadShortWithByte reads and returns Short by reading two Bytes.
func ReadShortWithByte(b *bytes.Buffer) Short {
	return Short(ReadByte(b))<<8 | Short(ReadByte(b))
}

// BenchmarkReadIntWithByte creates and refills buffer (with B.Timer stopped)
// so it can read Int values from it by using ReadIntWithByte.
func BenchmarkReadIntWithByte(b *testing.B) {
	buf := fullBuffer(100000)
	var r Int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadIntWithByte(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer(100000)
			b.StartTimer()
		}
	}
	result = r
	// It removes unused variable warning.
	_ = result
}

// BenchmarkReadIntWithShort creates and refills buffer (with B.Timer stopped)
// so it can read Int values from it by using ReadIntWithShort.
func BenchmarkReadIntWithShort(b *testing.B) {
	buf := fullBuffer(100000)
	var r Int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadIntWithShort(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer(100000)
			b.StartTimer()
		}
	}
	result = r
	// It removes unused variable warning.
	_ = result
}

// BenchmarkReadIntWithSlice creates and refills buffer (with B.Timer stopped)
// so it can read Int values from it by using ReadIntWithSlice.
func BenchmarkReadIntWithSlice(b *testing.B) {
	buf := fullBuffer(100000)
	var r Int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadIntWithSlice(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer(100000)
			b.StartTimer()
		}
	}
	result = r
}

// BenchmarkReadIntWithSliceNoAlloc creates and refills buffer (with B.Timer stopped)
// so it can read Int values from it by using ReadIntWithSliceNoAlloc.
func BenchmarkReadIntWithSliceNoAlloc(b *testing.B) {
	buf := fullBuffer(100000)
	var r Int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadIntWithSliceNoAlloc(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer(100000)
			b.StartTimer()
		}
	}
	result = r
}

// BenchmarkReadShortWithSlice creates and refills buffer (with B.Timer stopped)
// so it can read Short values from it by using ReadShortWithSlice.
func BenchmarkReadShortWithSlice(b *testing.B) {
	buf := fullBuffer(100000)
	var r Short
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadShortWithSlice(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer(100000)
			b.StartTimer()
		}
	}
	result = Int(r)
}

// BenchmarkReadShortWithSliceNoAlloc creates and refills buffer (with B.Timer stopped)
// so it can read Short values from it by using ReadShortWithSliceNoAlloc.
func BenchmarkReadShortWithSliceNoAlloc(b *testing.B) {
	buf := fullBuffer(100000)
	var r Short
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadShortWithSliceNoAlloc(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer(100000)
			b.StartTimer()
		}
	}
	result = Int(r)
}

// BenchmarkReadShortWithByte creates and refills buffer (with B.Timer stopped)
// so it can read Short values from it by using ReadShortWithByte.
func BenchmarkReadShortWithByte(b *testing.B) {
	buf := fullBuffer(100000)
	var r Short
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadShortWithByte(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer(100000)
			b.StartTimer()
		}
	}
	result = Int(r)
}
