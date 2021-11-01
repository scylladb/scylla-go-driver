package frame

func SWriteByte(b byte, buf *[]byte) error {
	//TODO https://pkg.go.dev/builtin#append doesn't return any error, undefined behavior when we don't have memory?
	*buf = append(*buf, b)
	return nil
}

// WriteShort writes [short] in a BigEndian order.
func SWriteShort(s uint16, buf *[]byte) error {
	// It might not look great, but it's similar to original implementation.
	// https://golang.org/src/encoding/binary/binary.go
	*buf = append(*buf,
		byte(s >> 8),
		byte(s),
	)
	return nil
}

// WriteInt writes [int] in a BigEndian order.
func SWriteInt(s int32, buf *[]byte) error {
	// It might not look great, but it's similar to original implementation.
	// https://golang.org/src/encoding/binary/binary.go
	*buf = append(*buf,
		byte(s >> 24),
		byte(s >> 16),
		byte(s >> 8),
		byte(s),
	)
	return nil
}

// WriteString writes [string] consisting of its length (in BE order)
// and the actual string.
func SWriteString(s string, buf *[]byte) error {
	// Write length of [string].
	err := SWriteShort(uint16(len(s)), buf)
	//TODO is this even needed with TODO at line 6?
	if err != nil {
		return err
	}
	*buf = append(*buf, []byte(s)...)
	return nil
}

func SWriteStringList(strLst []string, buf *[]byte) error {
	// Write the length of [string list].
	err := SWriteShort(uint16(len(strLst)), buf)
	//TODO is this even needed with TODO at line 6?
	if err != nil {
		return err
	}

	// Write [string list]
	for _, s := range strLst {
		err = SWriteString(s, buf)
		if err != nil {
			return err
		}
	}
	return nil
}

func SWriteStringMultiMap(m StringMultiMap, buf *[]byte) error {
	err := SWriteShort(uint16(len(m)),buf) // Write the number of elements in map.
	if err != nil {
		return nil
	}

	for key, strLst := range m {
		err = SWriteString(key, buf) // Write <key>.
		if err != nil {
			return err
		}

		err = SWriteStringList(strLst, buf) // Write <value>.
		if err != nil {
			return err
		}
	}
	return nil
}
