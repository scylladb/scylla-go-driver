package frame

const (
	OpError         Byte = 0x00
	OpStartup       Byte = 0x01
	OpReady         Byte = 0x02
	OpAuthenticate  Byte = 0x03
	OpOptions       Byte = 0x05
	OpSupported     Byte = 0x06
	OpQuery         Byte = 0x07
	OpResult        Byte = 0x08
	OpPrepare       Byte = 0x09
	OpExecute       Byte = 0x0A
	OpRegister      Byte = 0x0B
	OpEvent         Byte = 0x0C
	OpBatch         Byte = 0x0D
	OpAuthChallenge Byte = 0x0E
	OpAuthResponse  Byte = 0x0F
	OpAuthSuccess   Byte = 0x10
)
