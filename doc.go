/*
Package scylla implements an efficient shard-aware driver for ScyllaDB.

# Connecting to the cluster

Pass a keyspace and a list of initial node IP addresses to DefaultSessionConfig to create a new cluster configuration:

	cfg := scylla.DefaultSessionConfig("keyspace", "192.168.1.1", "192.168.1.2", "192.168.1.3")

Port can be specified as part of the address, the above is equivalent to:

	cfg := scylla.DefaultSessionConfig("192.168.1.1:9042", "192.168.1.2:9042", "192.168.1.3:9042")

It is recommended to use the value set in the Scylla config for broadcast_address or listen_address,
an IP address not a domain name. This is because events from Scylla will use the configured IP
address, which is used to index connected hosts.

Then you can customize more options (see SessionConfig):

	cfg.Keyspace = "example"
	cfg.Consistency = scylla.QUORUM

When ready, create a session from the configuration and context.Context,
once the context is done session will close automatically,
stopping requests from being sent and new connections from being made.

Don't forget to Close the session once you are done with it and not sure context will be done:

	session, err := scylla.CreateSession(context.Background(), cfg)
	if err != nil {
		return err
	}
	defer session.Close()

# Authentication

CQL protocol uses a SASL-based authentication mechanism and so consists of an exchange of server challenges and
client response pairs. The details of the exchanged messages depend on the authenticator used.

Currently the driver supports only default password authenticator which can be used like this:

	cfg := scylla.DefaultSessionConfig("keyspace", "192.168.1.1", "192.168.1.2", "192.168.1.3")
	cfg.Username = "user"
	cfg.Password = "password"
	session, err := scylla.CreateSession(context.Background(), cfg)
	if err != nil {
		return err
	}
	defer session.Close()

# Transport layer security

It is possible to secure traffic between the client and server with TLS, to do so just pass
your tls.Config to session config.

For example:

	cfg := scylla.DefaultSessionConfig("keyspace", "192.168.1.1", "192.168.1.2", "192.168.1.3")
	cfg.TLSConfig = &tls.Config{
		...
	}
	session, err := scylla.CreateSession(context.Background(), cfg)
	if err != nil {
		return err
	}
	defer session.Close()

# Data-center awareness and query routing

The driver by default will route prepared queries to nodes that hold data replicas based on partition key,
and non-prepared queries in a round-robin fashion.

To route queries to local DC first, use TokenAwareDCAwarePolicy. For example, if the datacenter you
want to primarily connect is called dc1 (as configured in the database):

	cfg := scylla.DefaultSessionConfig("keyspace", "192.168.1.1", "192.168.1.2", "192.168.1.3")
	cfg.HostSelectionPolicy = NewTokenAwareDCAwarePolicy("dc1")

The driver can only use token-aware routing for queries where all partition key columns are query parameters.
For example, instead of

	session.Query("select value from mytable where pk1 = 'abc' AND pk2 = ?")

use

	session.Query("select value from mytable where pk1 = ? AND pk2 = ?")

# Executing queries

Create queries with Session.Query. Query values can be reused between different but must not be
modified during executions of the query.

To execute a query use Query.Exec:

	q := session.Query(`INSERT INTO tweet (timeline, id, text) VALUES ("me", ?, "hello world")`,
	_, err := q.BindInt64(0, 2022).Exec(ctx)

Result rows can be read like this

	q := session.Query("SELECT name FROM names WHERE pk=?")
	result, err := q.BindInt64(0, 2022).Exec(ctx)
	fmt.Println(len(result.Rows))
	fmt.Println(result.Rows[0][0].AsText())

See Example for complete example.

# Prepared statements

The driver can prepare DML queries (SELECT/INSERT/UPDATE/DELETE/BATCH statements).
CQL protocol does not support preparing other query types.

# Executing multiple queries concurrently

Session is safe to use from multiple goroutines, so to execute multiple concurrent queries, just execute them
from several worker goroutines. Gocql provides synchronously-looking API (as recommended for Go APIs) and the queries
are executed asynchronously at the protocol level.

# Paging

The driver supports paging of results with automatic prefetch of 1 page, see Query.PageSize and Query.Iter.

It is also possible to control the paging manually with Query.PageState.
Manual paging is useful if you want to store the page state externally, for example in a URL to allow users
browse pages in a result. You might want to sign/encrypt the paging state when exposing it externally since
it contains data from primary keys.

Paging state is specific to the CQL protocol version and the exact query used. It is meant as opaque state that
should not be modified. If you send paging state from different query or protocol version, then the behaviour
is not defined (you might get unexpected results or an error from the server). For example, do not send paging state
returned by node using protocol version 3 to a node using protocol version 4. Also, when using protocol version 4,
paging state between Cassandra 2.2 and 3.0 is incompatible (https://issues.apache.org/jira/browse/CASSANDRA-10880).

The driver does not check whether the paging state is from the same protocol version/statement.
You might want to validate yourself as this could be a problem if you store paging state externally.
For example, if you store paging state in a URL, the URLs might become broken when you upgrade your cluster.

Call Query.PageState(nil) to fetch just the first page of the query results. Pass the page state returned in Result.PageState by
Query.Exec to Query.PageState of a subsequent query to get the next page. If the length of slice in Result.PageState is zero,
there are no more pages available (or an error occurred).

Using too low values of PageSize will negatively affect performance, a value below 100 is probably too low.
While Scylla returns exactly PageSize items (except for last page) in a page currently, the protocol authors
explicitly reserved the right to return smaller or larger amount of items in a page for performance reasons, so don't
rely on the page having the exact count of items.

# Retries

Queries can be marked as idempotent. Marking the query as idempotent tells the driver that the query can be executed
multiple times without affecting its result. Non-idempotent queries are not eligible for retrying nor speculative
execution.

Idempotent queries are retried in case of errors based on the configured RetryPolicy.

# Custom policies

If you need to use a custom Retry or HostSelectionPolicy please see the transport package documentation.
*/
package scylla
