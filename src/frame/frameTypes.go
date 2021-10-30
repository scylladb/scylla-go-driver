package frame

import (
	"errors"
	"io"
)

type Byte byte
type Short uint16
type Int int32
type Long int64
type Uuid [16]byte

type StringMultiMap map[string][]string

func WriteByte(b byte, writer io.Writer) (int64, error) {
	res, err := writer.Write([]byte{b})
	return int64(res), err
}

// WriteShort writes [short] in a BigEndian order.
func WriteShort(s Short, writer io.Writer) (int64, error) {
	// It might not look great, but it's similar to original implementation.
	// https://golang.org/src/encoding/binary/binary.go
	res, err := writer.Write([]byte{byte(s >> 8), byte(s)})
	return int64(res), err
}

// WriteInt writes [int] in a BigEndian order.
func WriteInt(s Int, writer io.Writer) (int64, error) {
	// It might not look great, but it's similar to original implementation.
	// https://golang.org/src/encoding/binary/binary.go
	res, err := writer.Write([]byte{
									byte(s >> 24),
									byte(s >> 16),
									byte(s >> 8),
									byte(s)},
									)
	return int64(res), err
}

// WriteString writes [string] consisting of its length (in BE order)
// and the actual string.
func WriteString(s string, writer io.Writer) (int64, error) {
	// Write length of [string].
	slen, err := WriteShort(Short(len(s)), writer)
	if err != nil {
		return 0, err
	}
	res, err := writer.Write([]byte(s))
	return int64(res) + slen, err
}

// WriteStringList writes [string list] consisting of
// [short] n, followed by n [string]s.
func WriteStringList(strLst []string, writer io.Writer) (int64, error) {
	// Write the length of [string list].
	wrote, err := WriteShort(Short(len(strLst)), writer)
	if err != nil {
		return 0, err
	}

	// Write [string list]
	for _, s := range strLst {
		res, err := WriteString(s, writer)
		wrote += res
		if err != nil {
			return wrote, err
		}
	}
	return wrote, err
}

// WriteStringMultiMap writes [string multimap] which consists of
// a [short] n, followed by n pair <k><v> where <k> is a [string]
// and <v> is a [string list].
// TODO: A lot of error checking in here, it would be great if we could reduce this.
func WriteStringMultiMap(m StringMultiMap, writer io.Writer) (int64, error) {
	wrote, err := WriteShort(Short(len(m)),writer) // Write the number of elements in map.
	if err != nil {
		return wrote, nil
	}

	for key, strLst := range m {
		res, err := WriteString(key, writer) // Write <key>.
		wrote += res
		if err != nil {
			return wrote, err
		}

		res, err = WriteStringList(strLst, writer) // Write <value>.
		wrote += res
		if err != nil {
			return wrote, err
		}
	}
	return wrote, err
}

func ReadByte(buf []byte) (b byte, err error) {
	if len(buf) == 0 {
		err = errors.New("not enough bytes to perform ReadByte")
	} else {
		b = buf[0]
		buf = buf[1:]
	}
	return
}

// ReadShort - Reads [short] from bytes stream in BigEndian order.
func ReadShort(buf []byte) (s Short, err error) {
	if len(buf) < 2 {
		err = errors.New("not enough bytes to perform ReadShort")
	} else {
		s = Short(buf[1]) | Short(buf[0]) << 8
		buf = buf[2:]
	}
	return
}

// ReadInt - Reads [int] from bytes stream in BigEndian order.
func ReadInt(buf []byte) (i Int, err error) {
	if len(buf) < 4 {
		err = errors.New("not enough bytes to perform ReadInt")
	} else {
		i = Int(buf[3]) | Int(buf[2]) << 8 |Int(buf[1]) << 16 | Int(buf[0]) << 24
		buf = buf[4:]
	}
	return
}

// ReadString reads [string] consisting of its length (in BE order)
// and the actual string.
func ReadString(buf []byte) (s string, err error) {
	// Read length of [string].
	l, err := ReadShort(buf)
	if err != nil || len(buf) < int(l) {
		err = errors.New("not enough bytes in ReadString")
	} else {
		s = string(buf[:l])
		buf = buf[l:]
	}
	return
}

// ReadStringList reads [string list] consisting of
// [short] n, followed by n [string]s.
func ReadStringList(buf []byte) (strLst []string, err error) {
	// Read the length of [string list].
	l, err := ReadShort(buf)
	if err != nil {
		return
	}

	strLst = make([]string, 0, l) // TODO: Hubert tell the others about it.
	for i := Short(0); i < l; i++ {
		var str string
		str, err = ReadString(buf)
		if err != nil {
			return
		}
		strLst = append(strLst, str)
	}
	return
}

// ReadStringMultiMap - reads [string multimap] which consists of
// a [short] n, followed by n pair <k><v> where <k> is a [string]
// and <v> is a [string list].
// TODO: A lot of error checking in here, it would be great if we could reduce this.
func ReadStringMultiMap(buf []byte, m StringMultiMap) (err error) {
	length, err := ReadShort(buf) // Read the number of elements in map.
	if err != nil {
		return
	}

	var key string
	var strLst []string
	for i := Short(0); i < length; i++ {
		key, err = ReadString(buf) // Read <key>.
		if err != nil {
			return
		}

		strLst, err = ReadStringList(buf) // Read <value>.
		if err != nil {
			return
		}

		m[key] = strLst
	}
	return
}
