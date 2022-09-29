To client implementors: the driver is meant to support [CQL Binary Protocol v4] extended by the Duration type used by ScyllaDB.
Take those notes in mind when implementing new features.

## Components 
Cluster, Connection Pool, ConnReader, ConnWriter, are managed by their loops
after creation their contents should only be modified by the loops when possible, or with atomics.


## Queries
A single query struct can be used multiple times with different bindings.

Note: Creating Iters and AsyncQueries deep copies the query contents that can be modified after its creation.

## Iter
Iter.Next() returns query rows, driver asynchronically fetches the next page.

Iter.Close() returns an error to enable gocql wrappability.

## Retry Policies
```go
type RetryDecision byte

const (
	RetrySameNode RetryDecision = iota
	RetryNextNode
	DontRetry
)

type RetryPolicy interface {
    NewRetryDecider() transport.RetryDecider
}

// RetryDecider should be used for just one query that we want to retry.
// After that it should be discarded or reset.
type RetryDecider interface {
	Decide(RetryInfo) RetryDecision
	Reset()
}
```

Additionally to the decisions made by retry policy, if the driver routes the query to a node that is marked as DOWN, or no connection to this node is available, the driver will automatically retry the query on the next node supplied by host selection policy.


## Host Selection policies
```go
type HostSelectionPolicy interface {
     Node(QueryInfo, int) *Node 
}
```
QueryInfo should contain all needed information to route the query, including token, topology, keyspaces, etc.

QueryInfo also has a policyInfo field, which is preprocessed each time the driver changes its cluster topology, this is used by TokenAwarePolicy to avoid allocations in the hot path.

## Topology
- cluster's topology contents should be treated as read-only by the user,
    it should be managed by cluster loop, which should perform read-copy-update when refreshing topology / handling events

## Closing
- Session
  - closing is initiated by calling session.Close, or context being done
  - doesn't close Iters when its being closed, its user's responsibility
  - on close asks cluster to close
- Cluster
  - closing is initiated by sending an event to cluster.loop
  - on close asks all live nodes to close
  - can also close individual nodes if node was removed from `system.local / system.peers`, this is checked by `cluster.refreshTopology`
- Node / Pool
  - closing is initiated by sending an event to `poolRefiller.loop`
  - on close asks all owned connections to close
- Conn
  - closing is initiated by sending `_connCloseRequest` to `connWriter.loop`, session context being done, or an I/O error in either of conn loops
  - on close underlying TCP connection will be closed, leading to both conn loops stopping due to I/O errors, sending error responses to all `requestHandlers` that were registered before the close.

## Contexts
Context can be passed to session, making it cancel all operations after context is done. 

A context can be also passed to query execution methods, cancelling an individual request if the context is done before it was sent to the database.

[CQL Binary Protocol v4]: https://github.com/apache/cassandra/blob/35578a4a9f6a5614a388939354fe68d03dc20459/doc/native_protocol_v4.spec