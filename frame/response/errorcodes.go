package response

import "scylla-go-driver/frame"

type ErrorCode = frame.Int

// See CQL Binary Protocol v5, section 8 for more details.
// https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec
const (
	// ErrCodeServer indicates unexpected error on server-side.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1246-L1247
	ErrCodeServer ErrorCode = 0x0000

	// ErrCodeProtocol indicates a protocol violation by some client message.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1248-L1250
	ErrCodeProtocol ErrorCode = 0x000A

	// ErrCodeCredentials indicates missing required authentication.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1251-L1254
	ErrCodeCredentials ErrorCode = 0x0100

	// ErrCodeUnavailable indicates unavailable error.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1255-L1265
	ErrCodeUnavailable ErrorCode = 0x1000

	// ErrCodeOverloaded returned in case of request on overloaded node coordinator.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1266-L1267
	ErrCodeOverloaded ErrorCode = 0x1001

	// ErrCodeBootstrapping returned from the coordinator node in bootstrapping phase.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1268-L1269
	ErrCodeBootstrapping ErrorCode = 0x1002

	// ErrCodeTruncate indicates truncation exception.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1270
	ErrCodeTruncate ErrorCode = 0x1003

	// ErrCodeWriteTimeout returned in case of timeout during the request write.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1271-L1304
	ErrCodeWriteTimeout ErrorCode = 0x1100

	// ErrCodeReadTimeout returned in case of timeout during the request read.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1305-L1321
	ErrCodeReadTimeout ErrorCode = 0x1200

	// ErrCodeReadFailure indicates request read error which is not covered by ErrCodeReadTimeout.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1322-L1340
	ErrCodeReadFailure ErrorCode = 0x1300

	// ErrCodeFunctionFailure indicates an error in user-defined function.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1341-L1347
	ErrCodeFunctionFailure ErrorCode = 0x1400

	// ErrCodeWriteFailure indicates request write error which is not covered by ErrCodeWriteTimeout.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1348-L1385
	ErrCodeWriteFailure ErrorCode = 0x1500

	// ErrCodeCDCWriteFailure is defined, but not yet documented in CQLv5 protocol.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1386
	ErrCodeCDCWriteFailure ErrorCode = 0x160
	// ErrCodeCASWriteUnknown indicates only partially completed CAS operation.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1387-L1397
	ErrCodeCASWriteUnknown ErrorCode = 0x1700

	// ErrCodeSyntax indicates the syntax error in the query.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1399
	ErrCodeSyntax ErrorCode = 0x2000

	// ErrCodeUnauthorized indicates access rights violation by user on performed operation.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1400-L1401
	ErrCodeUnauthorized ErrorCode = 0x2100

	// ErrCodeInvalid indicates invalid query error which is not covered by ErrCodeSyntax.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1402
	ErrCodeInvalid ErrorCode = 0x2200

	// ErrCodeConfig indicates the configuration error.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1403
	ErrCodeConfig ErrorCode = 0x2300

	// ErrCodeAlreadyExists is returned for the requests creating the existing keyspace/table.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1404-L1413
	ErrCodeAlreadyExists ErrorCode = 0x2400

	// ErrCodeUnprepared returned from the host for prepared statement which is unknown.
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1414-L1417
	ErrCodeUnprepared ErrorCode = 0x2500
)
