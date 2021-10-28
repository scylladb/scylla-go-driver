package frame

type Header struct {
	version    Byte
	flags      Byte
	streamId   Short
	opcode     Byte
	length     Int
}

