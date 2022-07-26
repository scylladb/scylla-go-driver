package scylla

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
	"github.com/mmatczuk/scylla-go-driver/transport"
)

var n = 1000

func BenchmarkBindConcrete(b *testing.B) {
	q := Query{
		stmt: transport.Statement{
			Values: []frame.Value{
				{
					Type: &frame.Option{
						ID: frame.BigIntID,
					},
				},
			},
			Metadata: &frame.ResultMetadata{},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.BindInt64(0, int64(i))
	}

	v := frame.CqlValue{
		Type: &frame.Option{
			ID: frame.BigIntID,
		},
		Value: q.stmt.Values[0].Bytes,
	}

	if val, err := v.AsInt64(); err != nil {
		b.Fatal(err, q.err)
	} else if val != int64(b.N-1) {
		b.Fatal(q.err)
	}
}

func BenchmarkBindAny(b *testing.B) {
	q := Query{
		stmt: transport.Statement{
			Values: []frame.Value{
				{
					Type: &frame.Option{
						ID: frame.BigIntID,
					},
				},
			},
			Metadata: &frame.ResultMetadata{},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.BindAny(0, int64(i))
	}

	v := frame.CqlValue{
		Type: &frame.Option{
			ID: frame.BigIntID,
		},
		Value: q.stmt.Values[0].Bytes,
	}

	if val, err := v.AsInt64(); err != nil {
		b.Fatal(err, q.err)
	} else if val != int64(b.N-1) {
		b.Fatal(q.err)
	}
}

func BenchmarkAsConcrete(b *testing.B) {
	cqlVal := frame.CqlValue{
		Type: &frame.Option{
			ID: frame.BigIntID,
		},
		Value: make([]byte, 8),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, err := cqlVal.AsInt64()
		if err != nil {
			b.Fatal(err)
		}
		n = int(v)
	}

	if n != 0 {
		b.Fatal()
	}
}

func BenchmarkAsAny(b *testing.B) {
	cqlVal := frame.CqlValue{
		Type: &frame.Option{
			ID: frame.BigIntID,
		},
		Value: make([]byte, 8),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var v int64
		err := cqlVal.Unmarshal(&v)
		if err != nil {
			b.Fatal(err)
		}
		n = int(v)
	}

	if n != 0 {
		b.Fatal()
	}
}
