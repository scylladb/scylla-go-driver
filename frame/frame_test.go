package frame

import (
	"bytes"
	"testing"
)

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

// ReadShortWithSlice reads and returns Short by reading
// all two bytes at once to allocated byte slice.
func ReadShortWithSlice(b *bytes.Buffer) Short {
	tmp := make([]byte, 2)
	_, _ = b.Read(tmp)
	return Short(tmp[0])<<8 | Short(tmp[1])
}

// ReadIntWithShort reads and returns Int by reading two Shorts.
func ReadIntWithShort(b *bytes.Buffer) Int {
	return Int(ReadShortWithByte(b))<<16 | Int(ReadShortWithByte(b))
}

// ReadShortWithByte reads and returns Short by reading two Bytes.
func ReadShortWithByte(b *bytes.Buffer) Short {
	return Short(ReadByte(b))<<8 | Short(ReadByte(b))
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
	r = result
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
