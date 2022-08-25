package scylla

import "github.com/mmatczuk/scylla-go-driver/frame"

type BigInt int64

func (v BigInt) CqlValue() frame.CqlValue {
	return frame.CqlValue{
		Type: &frame.Option{ID: frame.BigIntID},
		Value: []byte{
			byte(v >> 56),
			byte(v >> 48),
			byte(v >> 40),
			byte(v >> 32),
			byte(v >> 24),
			byte(v >> 16),
			byte(v >> 8),
			byte(v),
		},
	}
}

func (v BigInt) Bytes() []byte {
	return []byte{
		byte(v >> 56),
		byte(v >> 48),
		byte(v >> 40),
		byte(v >> 32),
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	}
}
func (v BigInt) AppendBytes(dst []byte) []byte {
	return append(dst, v.Bytes()...)
}

func (v BigInt) CompareType(typ *frame.Option) bool {
	return typ.ID == frame.BigIntID
}

type List[T CqlValue] []T

func (v List[T]) CqlValue() frame.CqlValue {
	return frame.CqlValue{}
}

func (v List[T]) Bytes() []byte {
	if v == nil {
		return nil
	}

	l := len(v)
	res := []byte{byte(l << 24), byte(l << 16), byte(l << 8), byte(l)}
	for _, elem := range v {
		res = elem.AppendBytes(res)
	}

	return res
}

func (v List[T]) AppendBytes(dst []byte) []byte {
	return append(dst, v.Bytes()...)
}

func (v List[T]) CompareType(typ *frame.Option) bool {
	if typ.ID != frame.ListID || typ.List == nil {
		return false
	}

	if len(v) == 0 {
		var elem T
		return elem.CompareType(&typ.List.Element)
	}

	return v[0].CompareType(&typ.List.Element)
}

type Text string

func (v Text) CqlValue() frame.CqlValue {
	return frame.CqlValue{}
}

func (v Text) Bytes() []byte {
	return []byte(v)
}

func (v Text) AppendBytes(dst []byte) []byte {
	l := len(v)
	dst = append(dst, byte(l<<24), byte(l<<16), byte(l<<8), byte(l))
	return append(dst, v...)
}

func (v Text) CompareType(typ *frame.Option) bool {
	return typ.ID == frame.VarcharID
}
