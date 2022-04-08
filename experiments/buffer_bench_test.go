package experiments

import (
	"bytes"
	"testing"
)

type Short = uint16

// Result ensures that compiler won't skip operations
// during optimization of the benchmark functions.
// That's the reason why functions assign value to it.

var result int

// fullBuffer creates and returns buffer of length N
// that is filled with Bytes of consecutive values.
func fullBuffer() *bytes.Buffer {
	buf := &bytes.Buffer{}
	for i := 0; i <= 100000; i++ {
		buf.WriteByte(byte(i % 255))
	}
	return buf
}

// ReadIntWithSlice reads and returns Int by reading
// all four Bytes at once to allocated byte slice.
func ReadIntWithSlice(b *bytes.Buffer) int {
	tmp := make([]byte, 4)
	_, err := b.Read(tmp)
	if err != nil {
		panic(err)
	}
	return int(tmp[0])<<24 |
		int(tmp[1])<<16 |
		int(tmp[2])<<8 |
		int(tmp[3])
}

// ReadIntWithSliceNoAlloc reads and returns Int by reading
// all four Bytes at once to byte slice.
func ReadIntWithSliceNoAlloc(b *bytes.Buffer) int {
	tmp := []byte{0, 0, 0, 0}
	_, err := b.Read(tmp)
	if err != nil {
		panic(err)
	}
	return int(tmp[0])<<24 |
		int(tmp[1])<<16 |
		int(tmp[2])<<8 |
		int(tmp[3])
}

// ReadIntWithArray reads and returns Int by reading
// all four Bytes at once.
func ReadIntWithArray(b *bytes.Buffer) int {
	tmp := [4]byte{0, 0, 0, 0}
	_, err := b.Read(tmp[:])
	if err != nil {
		panic(err)
	}
	return int(tmp[0])<<24 |
		int(tmp[1])<<16 |
		int(tmp[2])<<8 |
		int(tmp[3])
}

// ReadShortWithSlice reads and returns Short by reading
// all two Bytes at once to allocated byte slice.
func ReadShortWithSlice(b *bytes.Buffer) Short {
	tmp := make([]byte, 2)
	_, err := b.Read(tmp)
	if err != nil {
		panic(err)
	}
	return Short(tmp[0])<<8 | Short(tmp[1])
}

// ReadShortWithSliceNoAlloc reads and returns Short by reading
// all two Bytes at once to allocated byte slice.
func ReadShortWithSliceNoAlloc(b *bytes.Buffer) Short {
	tmp := []byte{0, 0}
	_, err := b.Read(tmp)
	if err != nil {
		panic(err)
	}
	return Short(tmp[0])<<8 | Short(tmp[1])
}

func ReadByte(b *bytes.Buffer) byte {
	r, err := b.ReadByte()
	if err != nil {
		panic(err)
	}
	return r
}

// ReadIntWithByte reads and returns Int by reading two Shorts.
func ReadIntWithByte(b *bytes.Buffer) int {
	return int(ReadByte(b))<<24 | int(ReadByte(b))<<16 | int(ReadByte(b))<<8 | int(ReadByte(b))
}

// ReadIntWithShort reads and returns Int by reading two Shorts.
func ReadIntWithShort(b *bytes.Buffer) int {
	return int(ReadShortWithByte(b))<<16 | int(ReadShortWithByte(b))
}

// ReadShortWithByte reads and returns Short by reading two Bytes.
func ReadShortWithByte(b *bytes.Buffer) Short {
	return Short(ReadByte(b))<<8 | Short(ReadByte(b))
}

// BenchmarkReadIntWithByte creates and refills buffer (with B.Timer stopped)
// so it can read Int values from it by using ReadIntWithByte.
func BenchmarkReadIntWithByte(b *testing.B) {
	buf := fullBuffer()
	var r int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadIntWithByte(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer()
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
	buf := fullBuffer()
	var r int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadIntWithShort(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer()
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
	buf := fullBuffer()
	var r int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadIntWithSlice(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer()
			b.StartTimer()
		}
	}
	result = r
}

// BenchmarkReadIntWithSliceNoAlloc creates and refills buffer (with B.Timer stopped)
// so it can read Int values from it by using ReadIntWithSliceNoAlloc.
func BenchmarkReadIntWithSliceNoAlloc(b *testing.B) {
	buf := fullBuffer()
	var r int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadIntWithSliceNoAlloc(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer()
			b.StartTimer()
		}
	}
	result = r
}

// BenchmarkReadIntWithSliceNoAlloc creates and refills buffer (with B.Timer stopped)
// so it can read Int values from it by using ReadIntWithSliceNoAlloc.
func BenchmarkReadIntWithArray(b *testing.B) {
	buf := fullBuffer()
	var r int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadIntWithArray(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer()
			b.StartTimer()
		}
	}
	result = r
}

// BenchmarkReadShortWithSlice creates and refills buffer (with B.Timer stopped)
// so it can read Short values from it by using ReadShortWithSlice.
func BenchmarkReadShortWithSlice(b *testing.B) {
	buf := fullBuffer()
	var r Short
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadShortWithSlice(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer()
			b.StartTimer()
		}
	}
	result = int(r)
}

// BenchmarkReadShortWithSliceNoAlloc creates and refills buffer (with B.Timer stopped)
// so it can read Short values from it by using ReadShortWithSliceNoAlloc.
func BenchmarkReadShortWithSliceNoAlloc(b *testing.B) {
	buf := fullBuffer()
	var r Short
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadShortWithSliceNoAlloc(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer()
			b.StartTimer()
		}
	}
	result = int(r)
}

// BenchmarkReadShortWithByte creates and refills buffer (with B.Timer stopped)
// so it can read Short values from it by using ReadShortWithByte.
func BenchmarkReadShortWithByte(b *testing.B) {
	buf := fullBuffer()
	var r Short
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r = ReadShortWithByte(buf)
		if buf.Len() == 0 {
			b.StopTimer()
			buf = fullBuffer()
			b.StartTimer()
		}
	}
	result = int(r)
}

/*
	Benchmark results:
	BenchmarkReadIntWithByte-4             	159304146	         7.55 ns/op
	BenchmarkReadIntWithShort-4            	100000000	        10.6 ns/op
	BenchmarkReadIntWithSlice-4            	160086836	         7.46 ns/op
	BenchmarkReadIntWithSliceNoAlloc-4     	158571973	         7.68 ns/op
	BenchmarkReadIntWithArray-4            	157595760	         7.75 ns/op
	BenchmarkReadShortWithSlice-4          	100000000	        10.7 ns/op
	BenchmarkReadShortWithSliceNoAlloc-4   	100000000	        11.4 ns/op
	BenchmarkReadShortWithByte-4           	204602593	         5.34 ns/op
*/
