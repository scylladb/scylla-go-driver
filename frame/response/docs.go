package response

// Package response implements functions and types used for handling
// all types of CQL binary protocol responses.
// Reading from buffer is done in Big Endian order.
// Responses spec https://github.com/apache/cassandra/blob/cede538e029e14dc6298025794c72cc100118953/doc/native_protocol_v4.spec#L492
