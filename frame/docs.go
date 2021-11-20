package frame

// Package frame implements generic functions for reading and writing types from CQL binary protocol.
// Reading from and writing to is done in Big Endian order.
// Methods with prefix Put/Get refer to frame.Buffer and Write/Read prefixes refer to bytes.Buffer.
// Frame currently supports v4 protocol https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec.
// PutX ignores errors since bytes.Buffer WriteX always returns nil.