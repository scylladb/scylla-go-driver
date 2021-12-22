package frame

import (
	"encoding/hex"
)

func ShortToBytes(x Short) []byte {
	var out Buffer
	out.WriteShort(x)
	return out.Bytes()
}

func IntToBytes(x Int) []byte {
	var out Buffer
	out.WriteInt(x)
	return out.Bytes()
}

func StringToBytes(x string) []byte {
	var out Buffer
	out.WriteString(x)
	return out.Bytes()
}

func LongStringToBytes(x string) []byte {
	var out Buffer
	out.WriteLongString(x)
	return out.Bytes()
}

func ByteToBytes(b Byte) []byte {
	var out Buffer
	out.WriteByte(b)
	return out.Bytes()
}

func MassAppendBytes(elems ...[]byte) []byte {
	var ans []byte
	for _, v := range elems {
		ans = append(ans, v...)
	}
	return ans
}

// HexStringToBytes does begin with string's length.
func HexStringToBytes(s string) []byte {
	tmp, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return tmp
}

func ValueToBytes(v Value) []byte {
	b := Buffer{}
	b.WriteValue(v)
	return b.Bytes()
}

func LongToBytes(l Long) []byte {
	b := Buffer{}
	b.WriteLong(l)
	return b.Bytes()
}

func BytesToShortBytes(b Bytes) []byte {
	var out Buffer
	out.WriteShortBytes(b)
	return out.Bytes()
}

func StringListToBytes(sl StringList) []byte {
	var out Buffer
	out.WriteStringList(sl)
	return out.Bytes()
}

func InetToBytes(i Inet) []byte {
	b := Buffer{}
	b.WriteInet(i)
	return b.Bytes()
}

func (b *Buffer) Bytes() []byte {
	return b.buf.Bytes()
}

func (b *Buffer) Reset() {
	b.buf.Reset()
}
