package request

// Package request implements functions and types used for handling
// all types of CQL binary protocol requests.
// Writing to buffer is done in Big Endian order.
// Request spec https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L280
