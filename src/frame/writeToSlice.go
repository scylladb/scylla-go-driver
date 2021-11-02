package frame

func WriteByte(b byte, buf *[]byte) {
	//TODO https://pkg.go.dev/builtin#append doesn't return any error, undefined behavior when we're out of memory?
	*buf = append(*buf, b)
}

func WriteShort(s uint16, buf *[]byte) {
	// It might not look great, but it's similar to original implementation.
	// https://golang.org/src/encoding/binary/binary.go
	*buf = append(*buf,
		byte(s>>8),
		byte(s),
	)
}

func WriteInt(s int32, buf *[]byte) {
	*buf = append(*buf,
		byte(s>>24),
		byte(s>>16),
		byte(s>>8),
		byte(s),
	)
}

func WriteString(s string, buf *[]byte) {
	// Write length of [string].
	WriteShort(uint16(len(s)), buf)

	*buf = append(*buf, []byte(s)...)
}

func WriteStringList(strLst StringList, buf *[]byte) {
	// Write the length of [string list].
	WriteShort(uint16(len(strLst)), buf)

	// Write [string list]
	for _, s := range strLst {
		WriteString(s, buf)
	}
}

func WriteStringMultiMap(m StringMultiMap, buf *[]byte) {
	WriteShort(uint16(len(m)), buf) // Write the number of elements in map.

	for key, strLst := range m {
		WriteString(key, buf) // Write <key>.

		WriteStringList(strLst, buf) // Write <value>.
	}
}
