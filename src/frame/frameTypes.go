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

// WriteShort - Writes Short in a BigEndian order.
func WriteShort(s Short, writer io.Writer) (int64, error) {
	// It might not look great, but it's similar to original implementation.
	// https://golang.org/src/encoding/binary/binary.go
	res, err := writer.Write([]byte{byte(s >> 8), byte(s)})
	return int64(res), err
}

func WriteString(s string, writer io.Writer) (int64, error) {
	res, err := writer.Write([]byte(s))
	return int64(res), err
}

// A lot of error checking in here, it would be great if we could reduce this.
func WriteStringMultiMap(m StringMultiMap, writer io.Writer) (int64, error) {
	var wrote int64 = 0
	res, err := WriteShort(Short(len(m)),writer) // Write number of elements in map.
	wrote += res
	if err != nil {
		return wrote, nil
	}

	for key, strLst := range m {
		res, err = WriteString(key, writer) // Write <key>.
		wrote += res
		if err != nil {
			return wrote, err
		}

		// Write number of strings in <value> - length of [string list].
		res, err = WriteShort(Short(len(strLst)), writer)
		wrote += res
		if err != nil {
			return wrote, err
		}

		// Write [string list]
		for _, s := range strLst {
			res, err = WriteString(s, writer)
			wrote += res
			if err != nil {
				return wrote, err
			}
		}
	}
	return wrote, nil
}

func ReadShort(buf []byte) (s Short, err error) {
	if len(buf) < 2 {
		err = errors.New("not enough bytes in ReadShort")
	} else {
		s = Short(buf[1] | (buf[0] << 8))
		buf = buf[2:]
	}
	return
}

func ReadString(buf []byte) (s string, err error) {
	length, err := ReadShort(buf)
	if err != nil && len(buf) < int(length) {
		err = errors.New("not enough bytes in ReadString")
	} else {
		s = string(buf[:length])
		buf = buf[length:]
	}
	return
}


// A lot of error checking in here, it would be great if we could reduce this.
func ReadStringMultiMap(buf []byte, m StringMultiMap) (err error) {
	length, err := ReadShort(buf)
	if err != nil {
		return
	}

	for i := Short(0); i < length; i++{
		key, err := ReadString(buf) // Read option key.
		if err != nil {
			return
		}

		// Read number of strings in <value> - length of [string list].
		optionsNumber, err := ReadShort(buf)
		if err != nil {
			return
		}
		strLst := make([]string, length)
		for j := Short(0); j < optionsNumber; j++ {
			option, err := ReadString(buf)
			if err != nil {
				return
			}
			strLst = append(strLst, option)
		}
		// Place values in map.
		m[key] = strLst
	}
	return
}
