package frame

import "errors"

func ReadByte(buf *[]byte) (b byte, err error) {
	if len(*buf) < 1 {
		err = errors.New("not enough bytes to perform ReadByte")
	} else {
		b = (*buf)[0]
		*buf = (*buf)[1:]
	}
	return
}

// ReadShort - Reads [short] from bytes stream in BigEndian order.
func ReadShort(buf *[]byte) (s uint16, err error) {
	if len(*buf) < 2 {
		err = errors.New("not enough bytes to perform ReadShort")
	} else {
		s = uint16((*buf)[1]) | uint16((*buf)[0])<<8
		*buf = (*buf)[2:]
	}
	return
}

// ReadInt - Reads [int] from bytes stream in BigEndian order.
func ReadInt(buf *[]byte) (i int32, err error) {
	if len(*buf) < 4 {
		err = errors.New("not enough bytes to perform ReadInt")
	} else {
		i = int32((*buf)[3]) | int32((*buf)[2])<<8 | int32((*buf)[1])<<16 | int32((*buf)[0])<<24
		*buf = (*buf)[4:]
	}
	return
}

// ReadString reads [string] consisting of its length (in BE order)
// and the actual string.
func ReadString(buf *[]byte) (s string, err error) {
	// Read length of [string].
	l, err := ReadShort(buf)
	if err != nil || len(*buf) < int(l) {
		err = errors.New("not enough bytes in ReadString")
	} else {
		s = string((*buf)[:l])
		*buf = (*buf)[l:]
	}
	return
}

// ReadStringList reads [string list] consisting of
// [short] n, followed by n [string]s.
func ReadStringList(buf *[]byte) (strLst StringList, err error) {
	// Read the length of [string list].
	l, err := ReadShort(buf)
	if err != nil {
		return
	}

	strLst = make(StringList, 0, l)
	for i := uint16(0); i < l; i++ {
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
func ReadStringMultiMap(buf *[]byte, m StringMultiMap) (err error) {
	length, err := ReadShort(buf) // Read the number of elements in map.
	if err != nil {
		return
	}

	var key string
	var strLst []string
	for i := uint16(0); i < length; i++ {
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
