package experiments

import (
	"sync/atomic"
	"testing"
	"unsafe"
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
