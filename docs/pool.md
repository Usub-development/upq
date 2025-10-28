# PgPool

`PgPool` is the global connection pool for asynchronous PostgreSQL operations.  
It manages a set of live connections (`PGconn` + non-blocking TCP socket), distributes them among coroutines, and
automatically recycles idle ones.

It is the primary interface for all query execution in **upq**.

---

## Overview

`PgPool` implements:

- **connection pooling** (limited by `max_pool_size`)
- **non-blocking acquisition** with exponential retry
- **automatic recycling** of connections after each query
- **error propagation** through `QueryResult` with full diagnostics

It guarantees **no blocking**, **no busy-waiting**, and **safe use from multiple coroutines**.

---

## Initialization

```cpp
usub::pg::PgPool::init_global(
    host, port, user, db, password,
    max_pool_size, queue_capacity
);
```

Example:

```cpp
usub::pg::PgPool::init_global(
    "localhost",
    "5432",
    "postgres",
    "mydb",
    "password",
    32,  // max_pool_size
    64   // queue_capacity
);

auto& pool = usub::pg::PgPool::instance();
```

This must be called **once** during startup.
`instance()` returns the global singleton thereafter.

---

## Architecture

| Component        | Description                                                |
|------------------|------------------------------------------------------------|
| `idle_`          | Lock-free queue of reusable `PgConnectionLibpq` objects.   |
| `max_pool_`      | Hard limit of simultaneously live connections.             |
| `live_count_`    | Atomic counter of active PostgreSQL sessions.              |
| `queue_capacity` | Bounded size of idle queue before connections are dropped. |

Connections are created lazily and recycled when possible.
If the pool is empty and under the `max_pool_size`, a new connection is created asynchronously.

---

## Connection lifecycle

1. Coroutine requests a connection via `acquire_connection()`.
2. If one is available in the queue — it’s reused.
3. If not, and below limit — a new connection is created via `libpq`.
4. When coroutine finishes — it calls `release_connection(conn)`.

If the connection is invalid or closed, it’s automatically discarded.

---

## Manual connection control

Manual control is useful for:

* multi-step sequences on a single connection
* dedicated `LISTEN/NOTIFY` listeners
* long-running transactional sessions

```cpp
task::Awaitable<void> manual_usage()
{
    auto& pool = usub::pg::PgPool::instance();
    auto conn = co_await pool.acquire_connection();

    if (!conn || !conn->connected())
    {
        std::cout << "Connection unavailable\n";
        co_return;
    }

    auto res = co_await pool.query_on(conn, "SELECT now();");
    if (res.ok && !res.rows.empty())
        std::cout << "now() = " << res.rows[0].cols[0] << "\n";

    pool.release_connection(conn);
    co_return;
}
```

---

## Simplified API

For most use cases, prefer the high-level `query_awaitable()` interface:

```cpp
auto res = co_await pool.query_awaitable(
    "UPDATE users SET name = $1 WHERE id = $2 RETURNING name;",
    "John", 1
);
```

It automatically:

* acquires a connection,
* executes the query,
* releases it back to the pool.

---

## Error handling

All queries return a detailed [`QueryResult`](QueryResult.md) with structured diagnostics.

### Common error codes

| Code               | Meaning                           |
|--------------------|-----------------------------------|
| `OK`               | Successful operation              |
| `ConnectionClosed` | Connection lost or invalid        |
| `SocketReadFailed` | TCP I/O failure                   |
| `ServerError`      | PostgreSQL returned SQL error     |
| `InvalidFuture`    | Operation on inactive transaction |
| `Unknown`          | Uncategorized error               |

`PgPool` automatically detects connection loss, invalid sockets, and I/O errors — you do **not** need to manually check
`conn->connected()`.

---

## Concurrency behavior

`PgPool` is coroutine-safe and supports thousands of concurrent queries.

### Internals

* Each connection uses `libpq` in **non-blocking mode**.
* The event loop waits for socket readiness via `uvent`.
* Queries are executed concurrently without locks — connections are taken and returned atomically through `MPMCQueue`.

### Example: parallel queries

```cpp
task::Awaitable<void> parallel_queries()
{
    auto& pool = usub::pg::PgPool::instance();

    auto t1 = usub::uvent::system::co_spawn(
        pool.query_awaitable("SELECT pg_sleep(1);")
    );

    auto t2 = usub::uvent::system::co_spawn(
        pool.query_awaitable("SELECT 42;")
    );

    co_await t1;
    co_await t2;
    co_return;
}
```

Each coroutine acquires a separate connection or waits asynchronously for one to become available.

---

## Pool exhaustion

If all connections are in use and the pool has reached `max_pool_size`,
new `acquire_connection()` calls **wait asynchronously** until a connection is released or a new slot opens.

This does not block the thread — the coroutine is suspended via the `uvent` scheduler.

---

## Queue capacity behavior

`queue_capacity` defines how many idle connections can be stored in the pool.

* When `idle_` is full and a connection is released → it’s discarded.
* When demand spikes again → new connections are created.

This provides natural backpressure and keeps memory footprint predictable.

---

## Error transparency (since v1.0.1)

All queries and internal operations now report structured diagnostic codes.

### Example: Connection loss

```cpp
auto res = co_await pool.query_awaitable("SELECT now();");
if (!res.ok && res.code == PgErrorCode::ConnectionClosed)
{
    std::cout << "Lost connection: " << res.error << "\n";
}
```

### Example: Server-side SQL error

```cpp
auto res = co_await pool.query_awaitable("SELECT * FROM missing;");
if (!res.ok && res.code == PgErrorCode::ServerError)
{
    std::cout
        << "Server error: " << res.error << "\n"
        << "SQLSTATE: " << res.server_sqlstate << "\n";
}
```

---

## Design notes

* `PgPool` is **lazy** — it doesn’t open all connections upfront.
  Connections are created on demand up to `max_pool_size`.

* Connections are **thread-affine** but coroutine-safe.
  Use a single pool instance per process — it’s fully concurrent.

* `PgPool` integrates tightly with `uvent` scheduler and non-blocking sockets.
  Every wait (connect, read, write) yields to the event loop.

---

## Best practices

- ✅ Initialize pool early (before spawning coroutines).
- ✅ Use `query_awaitable()` for short-lived queries.
- ✅ Use manual `acquire_connection()` for long-running listeners or transactions.
- ✅ Check `QueryResult.code` and `ok` for every query.
- ✅ Release connections when finished — don’t hold them across awaits.
- ❌ Don’t share one connection across threads.
- ❌ Don’t assume immediate connection creation — first queries may take longer.

---

## Summary

| Feature      | Description                               |
|--------------|-------------------------------------------|
| Asynchronous | Non-blocking coroutine API                |
| Safe         | Lock-free concurrent access               |
| Lazy         | Creates connections only as needed        |
| Bounded      | Max live connections controlled by config |
| Transparent  | Every error is classified and traceable   |

`PgPool` is the backbone of the upq runtime — it provides scalable, coroutine-friendly database access with
deterministic failure semantics.