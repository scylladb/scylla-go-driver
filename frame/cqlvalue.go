package frame

import (
	"fmt"
	"net"
	"unicode"
)

type CqlValue struct {
	Type  *Option
	Value Bytes
}

func (c CqlValue) AsASCII() (string, error) {
	if c.Type.ID != ASCIIID {
		return "", fmt.Errorf("%v is not of ASCII type", c)
	}

	for _, v := range c.Value {
		if v > unicode.MaxASCII {
			return "", fmt.Errorf("%v contains non-ascii characters", c)
		}
	}

	return string(c.Value), nil
}

func (c CqlValue) AsBoolean() (bool, error) {
	if c.Type.ID != BooleanID {
		return false, fmt.Errorf("%v is not of Boolean type", c)
	}

	return c.Value[0] != 0, nil
}

func (c CqlValue) AsBlob() ([]byte, error) {
	if c.Type.ID != BlobID {
		return nil, fmt.Errorf("%v is not of Blob type", c)
	}

	v := make([]byte, len(c.Value))
	copy(v, c.Value)
	return v, nil
}

func (c CqlValue) AsUUID() ([16]byte, error) {
	if c.Type.ID != UUIDID {
		return [16]byte{}, fmt.Errorf("%v is not of UUID type", c)
	}

	if len(c.Value) != 16 {
		return [16]byte{}, fmt.Errorf("expected 16 bytes, got %v", len(c.Value))
	}

	var v [16]byte
	copy(v[:], c.Value)
	return v, nil
}

func (c CqlValue) AsTimeUUID() ([16]byte, error) {
	if c.Type.ID != TimeUUIDID {
		return [16]byte{}, fmt.Errorf("%v is not of TimeUUID type", c)
	}

	if len(c.Value) != 16 {
		return [16]byte{}, fmt.Errorf("expected 16 bytes, got %v", len(c.Value))
	}

	var v [16]byte
	copy(v[:], c.Value)
	return v, nil
}

func (c CqlValue) AsInt() (int32, error) {
	if c.Type.ID != IntID {
		return 0, fmt.Errorf("%v is not of Int type", c)
	}

	if len(c.Value) != 4 {
		return 0, fmt.Errorf("expected 4 bytes, got %v", len(c.Value))
	}

	return int32(c.Value[0])<<24 |
		int32(c.Value[1])<<16 |
		int32(c.Value[2])<<8 |
		int32(c.Value[3]), nil
}

func (c CqlValue) AsSmallInt() (int16, error) {
	if c.Type.ID != SmallIntID {
		return 0, fmt.Errorf("%v is not of SmallInt type", c)
	}

	if len(c.Value) != 2 {
		return 0, fmt.Errorf("expected 2 bytes, got %v", len(c.Value))
	}

	return int16(c.Value[0])<<8 | int16(c.Value[1]), nil
}

func (c CqlValue) AsTinyInt() (int8, error) {
	if c.Type.ID != TinyIntID {
		return 0, fmt.Errorf("%v is not of TinyInt type", c)
	}

	if len(c.Value) != 1 {
		return 0, fmt.Errorf("expected 1 byte, got %v", len(c.Value))
	}

	return int8(c.Value[0]), nil
}

func (c CqlValue) AsBigInt() (int64, error) {
	if c.Type.ID != BigIntID {
		return 0, fmt.Errorf("%v is not of BigInt type", c)
	}

	if len(c.Value) != 8 {
		return 0, fmt.Errorf("expected 8 bytes, got %v", len(c.Value))
	}

	return int64(c.Value[0])<<56 |
		int64(c.Value[1])<<48 |
		int64(c.Value[2])<<40 |
		int64(c.Value[3])<<32 |
		int64(c.Value[4])<<24 |
		int64(c.Value[5])<<16 |
		int64(c.Value[6])<<8 |
		int64(c.Value[7]), nil
}

func (c CqlValue) AsText() (string, error) {
	if c.Type.ID != VarcharID {
		return "", fmt.Errorf("%v is not of Text/Varchar type", c)
	}

	return string(c.Value), nil
}

func (c CqlValue) AsInet() (net.IP, error) {
	if c.Type.ID != InetID {
		return nil, fmt.Errorf("%v is not of Inet type", c)
	}

	if len(c.Value) != 4 && len(c.Value) != 16 {
		return nil, fmt.Errorf("invalid ip length")
	}

	return net.IP(c.Value), nil
}

func CqlFromBlob(b []byte) CqlValue {
	return CqlValue{
		Type:  &Option{ID: BlobID},
		Value: b,
	}
}

func CqlFromInt(v int32) CqlValue {
	return CqlValue{
		Type: &Option{ID: IntID},
		Value: []byte{byte(v >> 24),
			byte(v >> 16),
			byte(v >> 8),
			byte(v)},
	}
}

func CqlFromText(s string) CqlValue {
	return CqlValue{
		Type:  &Option{ID: VarcharID},
		Value: []byte(s),
	}
}
