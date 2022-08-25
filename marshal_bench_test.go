package scylla

import (
	"reflect"
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
	"github.com/mmatczuk/scylla-go-driver/transport"
)

var samples = []Text{
	"a", "aa", "aaa", "aaaa", "aaaaa", "aaaaaaaaaa",
}

var samples2 = []string{
	"a", "aa", "aaa", "aaaa", "aaaaa", "aaaaaaaaaa",
}

func BenchmarkBindStringListConcrete(b *testing.B) {
	q := Query{
		stmt: transport.Statement{
			Values: []frame.Value{
				{
					Type: &frame.Option{
						ID: frame.ListID,
						List: &frame.ListOption{
							Element: frame.Option{
								ID: frame.VarcharID,
							},
						},
					},
				},
			},
			Metadata: &frame.ResultMetadata{},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.BindTextList(0, samples2)
	}

	v := frame.CqlValue{
		Type: &frame.Option{
			ID: frame.ListID,
			List: &frame.ListOption{
				Element: frame.Option{
					ID: frame.VarcharID,
				},
			},
		},
		Value: q.stmt.Values[0].Bytes,
	}

	if val, err := v.AsStringSlice(); err != nil {
		b.Fatal(err, q.err)
	} else if reflect.DeepEqual(val, samples) {
		b.Fatal(q.err)
	}
}

func BenchmarkBindStringListGeneric(b *testing.B) {
	q := Query{
		stmt: transport.Statement{
			Values: []frame.Value{
				{
					Type: &frame.Option{
						ID: frame.ListID,
						List: &frame.ListOption{
							Element: frame.Option{
								ID: frame.VarcharID,
							},
						},
					},
				},
			},
			Metadata: &frame.ResultMetadata{},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.BindCast2(0, List[Text](samples))
	}

	v := frame.CqlValue{
		Type: &frame.Option{
			ID: frame.ListID,
			List: &frame.ListOption{
				Element: frame.Option{
					ID: frame.VarcharID,
				},
			},
		},
		Value: q.stmt.Values[0].Bytes,
	}

	if val, err := v.AsStringSlice(); err != nil {
		b.Fatal(err, q.err)
	} else if reflect.DeepEqual(val, samples) {
		b.Fatal(q.err)
	}
}

func BenchmarkBindStringListAny(b *testing.B) {
	q := Query{
		stmt: transport.Statement{
			Values: []frame.Value{
				{
					Type: &frame.Option{
						ID: frame.ListID,
						List: &frame.ListOption{
							Element: frame.Option{
								ID: frame.VarcharID,
							},
						},
					},
				},
			},
			Metadata: &frame.ResultMetadata{},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.BindAny(0, samples2)
	}

	v := frame.CqlValue{
		Type: &frame.Option{
			ID: frame.ListID,
			List: &frame.ListOption{
				Element: frame.Option{
					ID: frame.VarcharID,
				},
			},
		},
		Value: q.stmt.Values[0].Bytes,
	}

	if val, err := v.AsStringSlice(); err != nil {
		b.Fatal(err, q.err)
	} else if reflect.DeepEqual(val, samples) {
		b.Fatal(q.err)
	}
}

func BenchmarkBindInt64Concrete(b *testing.B) {
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

func BenchmarkBindInt64ConcreteReuse(b *testing.B) {
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
		q.BindInt64Reusing(0, int64(i))
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

func BenchmarkBindInt64Bindable(b *testing.B) {
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
		q.BindCast2(0, BigInt(i))
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

func BenchmarkBindInt64Any(b *testing.B) {
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
