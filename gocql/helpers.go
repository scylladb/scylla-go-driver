package gocql

import (
	"math/big"
	"reflect"
	"time"

	"gopkg.in/inf.v0"
)

func copyBytes(p []byte) []byte {
	b := make([]byte, len(p))
	copy(b, p)
	return b
}

func appendInt(p []byte, n int32) []byte {
	return append(p, byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}

func readInt(p []byte) int32 {
	return int32(p[0])<<24 | int32(p[1])<<16 | int32(p[2])<<8 | int32(p[3])
}

func appendBytes(p []byte, d []byte) []byte {
	if d == nil {
		return appendInt(p, -1)
	}
	p = appendInt(p, int32(len(d)))
	p = append(p, d...)
	return p
}

func goType(t TypeInfo) reflect.Type {
	switch t.Type() {
	case TypeVarchar, TypeAscii, TypeInet, TypeText:
		return reflect.TypeOf(*new(string))
	case TypeBigInt, TypeCounter:
		return reflect.TypeOf(*new(int64))
	case TypeTime:
		return reflect.TypeOf(*new(time.Duration))
	case TypeTimestamp:
		return reflect.TypeOf(*new(time.Time))
	case TypeBlob:
		return reflect.TypeOf(*new([]byte))
	case TypeBoolean:
		return reflect.TypeOf(*new(bool))
	case TypeFloat:
		return reflect.TypeOf(*new(float32))
	case TypeDouble:
		return reflect.TypeOf(*new(float64))
	case TypeInt:
		return reflect.TypeOf(*new(int))
	case TypeSmallInt:
		return reflect.TypeOf(*new(int16))
	case TypeTinyInt:
		return reflect.TypeOf(*new(int8))
	case TypeDecimal:
		return reflect.TypeOf(*new(*inf.Dec))
	case TypeUUID, TypeTimeUUID:
		return reflect.TypeOf(*new(UUID))
	case TypeList, TypeSet:
		return reflect.SliceOf(goType(t.(CollectionType).Elem))
	case TypeMap:
		return reflect.MapOf(goType(t.(CollectionType).Key), goType(t.(CollectionType).Elem))
	case TypeVarint:
		return reflect.TypeOf(*new(*big.Int))
	case TypeTuple:
		// what can we do here? all there is to do is to make a list of interface{}
		tuple := t.(TupleTypeInfo)
		return reflect.TypeOf(make([]interface{}, len(tuple.Elems)))
	case TypeUDT:
		return reflect.TypeOf(make(map[string]interface{}))
	case TypeDate:
		return reflect.TypeOf(*new(time.Time))
	case TypeDuration:
		return reflect.TypeOf(*new(Duration))
	default:
		return nil
	}
}
