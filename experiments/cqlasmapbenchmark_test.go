package experiments

import (
	"fmt"
	"testing"

	"scylla-go-driver/frame"
)

func CqlAsMap[K comparable, V any](c frame.CqlValue) (map[K]V, error) {
	if c.Type.ID != frame.MapID {
		return nil, fmt.Errorf("is not of map type")
	}

	keyType := c.Type.Map.Value.ID
	valueType := c.Type.Map.Key.ID

	var buf frame.Buffer
	buf.Write(c.Value)

	size := buf.ReadInt()

	m := make(map[K]V, size)

	var key interface{}
	var value interface{}

	for i := frame.Int(0); i < size; i++ {
		switch keyType {
		case frame.IntID:
			key = buf.ReadInt()
		default:
			return nil, fmt.Errorf("unsupported key type")
		}

		switch valueType {
		case frame.IntID:
			value = buf.ReadInt()
		default:
			return nil, fmt.Errorf("unsupported value type")
		}

		if validKey, ok := key.(K); ok {
			if validValue, ok2 := value.(V); ok2 {
				m[validKey] = validValue
				continue
			}
		}

		return nil, fmt.Errorf("data can not be parsed to a specified type")
	}

	return m, buf.Error()
}

func CqlAsMapInt32Int32(c frame.CqlValue) (map[int32]int32, error) {
	if c.Type.ID != frame.MapID {
		return nil, fmt.Errorf("is not of map type")
	}

	if c.Type.Map.Key.ID != frame.IntID {
		return nil, fmt.Errorf("invalid key type")
	}

	if c.Type.Map.Value.ID != frame.IntID {
		return nil, fmt.Errorf("invalid value type")
	}

	var buf frame.Buffer
	buf.Write(c.Value)

	size := buf.ReadInt()

	m := make(map[int32]int32, size)

	for i := frame.Int(0); i < size; i++ {
		key := buf.ReadInt()
		value := buf.ReadInt()
		m[key] = value
	}

	return m, buf.Error()
}

func mapint32int32AsCql(n int32) frame.CqlValue {
	var buf frame.Buffer
	buf.WriteInt(n)
	for i := int32(1); i <= n; i++ {
		buf.WriteInt(i)
		buf.WriteInt(i)
	}

	return frame.CqlValue{
		Type: &frame.Option{
			ID:  frame.MapID,
			Map: &frame.MapOption{Key: frame.Option{ID: frame.IntID}, Value: frame.Option{ID: frame.IntID}},
		},
		Value: buf.Bytes(),
	}
}

var n int32 = 1000

func BenchmarkGenericMapInt32Int32(b *testing.B) {
	c := mapint32int32AsCql(n)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m, err := CqlAsMap[int32, int32](c)
		if err != nil {
			b.Fatal(err)
		}

		if int32(len(m)) != n {
			b.Fatalf("invalid length %v", len(m))
		}
	}
}

func BenchmarkMapInt32Int32(b *testing.B) {
	c := mapint32int32AsCql(n)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m, err := CqlAsMapInt32Int32(c)
		if err != nil {
			b.Fatal(err)
		}

		if int32(len(m)) != n {
			b.Fatalf("invalid length %v", len(m))
		}
	}
}
