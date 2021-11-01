package frame

func SWriteByte(b byte, buf *[]byte) {
	//TODO https://pkg.go.dev/builtin#append doesn't return any error, undefined behavior when we don't have memory?
	*buf = append(*buf, b)
}

func SWriteShort(s uint16, buf *[]byte) {
	// It might not look great, but it's similar to original implementation.
	// https://golang.org/src/encoding/binary/binary.go
	*buf = append(*buf,
		byte(s>>8),
		byte(s),
	)
}

func SWriteInt(s int32, buf *[]byte) {
	*buf = append(*buf,
		byte(s>>24),
		byte(s>>16),
		byte(s>>8),
		byte(s),
	)
}

func SWriteString(s string, buf *[]byte) {
	// Write length of [string].
	SWriteShort(uint16(len(s)), buf)

	*buf = append(*buf, []byte(s)...)
}

func SWriteStringList(strLst []string, buf *[]byte) {
	// Write the length of [string list].
	SWriteShort(uint16(len(strLst)), buf)

	// Write [string list]
	for _, s := range strLst {
		SWriteString(s, buf)
	}
}

func SWriteStringMultiMap(m StringMultiMap, buf *[]byte) {
	SWriteShort(uint16(len(m)), buf) // Write the number of elements in map.

	for key, strLst := range m {
		SWriteString(key, buf) // Write <key>.

		SWriteStringList(strLst, buf) // Write <value>.
	}
}
