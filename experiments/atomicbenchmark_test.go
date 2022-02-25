package experiments

import (
	"sync/atomic"
	"testing"
	"unsafe"

	uber "go.uber.org/atomic"
)

type data = map[string]int

var atomicResult data

type av struct {
	v atomic.Value
}

func (a *av) store(m data) {
	a.v.Store(m)
}

func (a *av) load() data {
	return a.v.Load().(data)
}

type up struct {
	v unsafe.Pointer
}

func (u *up) store(m data) {
	atomic.StorePointer(&u.v, unsafe.Pointer(&m))
}

func (u *up) load() data {
	return *(*data)(atomic.LoadPointer(&u.v))
}

type uberUP struct {
	v uber.UnsafePointer
}

func (u *uberUP) store(m data) {
	u.v.Store(unsafe.Pointer(&m))
}

func (u *uberUP) load() data {
	return *(*data)(u.v.Load())
}

type uberAV struct {
	v uber.Value
}

func (u *uberAV) store(m data) {
	u.v.Store(m)
}

func (u *uberAV) load() data {
	return u.v.Load().(data)
}

func BenchmarkLoadAV(b *testing.B) {
	var a av
	d := make(data)
	a.store(d)

	var temp data

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		temp = a.load()
	}

	atomicResult = temp
}

func BenchmarkLoadUP(b *testing.B) {
	var a up
	d := make(data)
	a.store(d)

	var temp data

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		temp = a.load()
	}

	atomicResult = temp
}

func BenchmarkLoadUberAV(b *testing.B) {
	var a uberAV
	d := make(data)
	a.store(d)

	var temp data

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		temp = a.load()
	}

	atomicResult = temp
}

func BenchmarkLoadUberUP(b *testing.B) {
	var a uberUP
	d := make(data)
	a.store(d)

	var temp data

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		temp = a.load()
	}

	atomicResult = temp
}
