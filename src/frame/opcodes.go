package frame

const (
	OpError         byte = 0x00
	OpStartup       byte = 0x01
	OpReady         byte = 0x02
	OpAuthenticate  byte = 0x03
	OpOptions       byte = 0x05
	OpSupported     byte = 0x06
	OpQuery         byte = 0x07
	OpResult        byte = 0x08
	OpPrepare       byte = 0x09
	OpExecute       byte = 0x0A
	OpRegister      byte = 0x0B
	OpEvent         byte = 0x0C
	OpBatch         byte = 0x0D
	OpAuthChallenge byte = 0x0E
	OpAuthResponse  byte = 0x0F
	OpAuthSuccess   byte = 0x10
)
