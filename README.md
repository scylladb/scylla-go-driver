# ScyllaDB Go Driver

This is a high-performance client-side driver for [ScyllaDB] written in pure Go.

**Note: this driver is currently in alpha. Bug reports and pull requests are welcome!**

## Installation
`go get github.com/scylladb/scylla-go-driver`
## Examples
```go
ctx := context.Background()

cfg := scylla.DefaultSessionConfig("exampleks", "192.168.100.100")
session, err := scylla.NewSession(ctx, cfg)
if err != nil {
	return err
}
defer session.Close()

requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
defer cancel()

q, err := session.Prepare(requestCtx, "SELECT id, name FROM exampleks.names WHERE id=?")
if err != nil {
	return err
}

res, err := q.BindInt64(0, 64).Exec(requestCtx)
```

Please see the full [example](examples/basic.go) program for more information.

All examples are available in the [examples](examples) directory

## Features and Roadmap

The driver supports the following:

* Session and query context support
* Token-aware routing
* Shard-aware routing (specific to ScyllaDB)
* Prepared statements
* Query paging
* CQL binary protocol version 4
* Configurable load balancing policies
* Configurable retry policies
* TLS support
* Authentication support
* Compression (LZ4 and Snappy algorithms)

Ongoing efforts:
* Gocql drop-in replacement
* More tests
* More benchmarks

Missing features:
* Cassandra support
* Batch statements
* Full CQL Events Support
* Support for all CQL types (Generic binding) 
* Speculative Execution
* CQL tracing
* Automatic node status updating
* Caching prepared statements
* Non-default keyspace token-aware query routing

## Supported Go Versions
Our driver's minimum supported Go version is 1.18

## Reference Documentation

* [CQL binary protocol] specification version 4

## License

This project is licensed under [Apache License, Version 2.0](LICENSE)


[CQL binary protocol]: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
[ScyllaDB]: https://www.scylladb.com/
