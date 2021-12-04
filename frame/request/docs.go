package request

// Package request implements functions and types used for handling
// all types of CQL binary protocol requests.
// Writing to buffer is done in Big Endian order.
// Request spec https://github.com/apache/cassandra/blob/951d72cd929d1f6c9329becbdd7604a9e709587b/doc/native_protocol_v4.spec#L280
