# PgPool

`PgPool` is the global connection pool for asynchronous PostgreSQL operations.
It manages a set of live connections (`PGconn` + non-blocking TCP socket), distributes them among coroutines, and
automatically recycles idle ones.

It is the primary interface for all query execution in **upq**.

---

## Overview

`PgPool` provides:

- Connection pooling (bounded by `max_pool_size`)
- Non-blocking acquisition (coroutines are suspended, not threads)
- Automatic recycling of idle connections
- Structured error propagation (`QueryResult`, `PgCopyResult`, `PgCursorChunk`)
- Optional health checking of the database
- Support for high-volume operations:
    - `COPY ... FROM STDIN` (bulk ingest)
    - `COPY ... TO STDOUT` (bulk export/streaming)
    - Server-side cursors with chunked fetch

---

## Initialization

```cpp
usub::pg::PgPool::init_global(
    host,
    port,
    user,
    db,
    password,
    /*max_pool_size*/ 32,
    /*queue_capacity*/ 64,
    usub::pg::PgPoolHealthConfig{
        .enabled = true,
        .interval_ms = 3000
    }
);
```

When `enabled = true`, the pool spawns a background coroutine that periodically validates connectivity by running
`SELECT 1;` using temporary connections.

You can check runtime stats:

```cpp
auto& hc = usub::pg::PgPool::instance().health_checker();
auto& stats = hc.stats();

std::cout
    << "checks=" << stats.iterations.load()
    << " ok=" << stats.ok_checks.load()
    << " failed=" << stats.failed_checks.load()
    << std::endl;
```

---

## Health checker behavior

`PgHealthChecker` runs inside the event loop and does:

1. Acquire a connection
2. Execute `SELECT 1`
3. Mark success/failure counters
4. Release the connection

It does not block and does not throw. It is purely diagnostic and early-failure detection.

Default `interval_ms`: `600000` (10 minutes). You can override it.

---

## Internal layout

| Field             | Description                                                     |
|-------------------|-----------------------------------------------------------------|
| `idle_`           | Lock-free MPMC queue of reusable `PgConnectionLibpq`            |
| `max_pool_`       | Max number of live connections allowed at once                  |
| `live_count_`     | Current number of established connections                       |
| `queue_capacity`  | Max number of idle connections retained; extra ones are dropped |
| `health_checker_` | Optional background watcher                                     |

Connections are created lazily. The pool never pre-allocates the full capacity.

---

## Connection lifecycle

1. Coroutine calls `acquire_connection()`.
2. Pool tries to dequeue an existing idle connection.
3. If none available and under `max_pool_size`, a new connection is opened asynchronously (non-blocking
   `PQconnectStart`/`PQconnectPoll`).
4. The coroutine uses the connection.
5. When done, it calls one of:

* `release_connection(conn)` (fast path)
* `release_connection_async(conn)` (safe drain path, see below)

The pool may drop a connection instead of recycling it if it is not considered reusable.

---

## Releasing connections and "dirty" connections

PostgreSQL + libpq is strictly single-flight per connection: you cannot pipeline multiple concurrent commands on the
same `PGconn` unless you have fully drained all pending results.

Because we run many coroutines, this matters.

### Problem

If coroutine A finishes a long sequence (e.g. cursor, COMMIT, COPY) and returns the connection to the pool,
`libpq` may still have unread `PGresult`s or pending input on the socket.
If we give that same connection to coroutine B immediately, B will hit:

```
another command is already in progress
```

This is not a logic error in user code — it’s a race between “application thinks it's done” vs “libpq still has unread
protocol frames”.

### Enforcement

The pool enforces “return only clean connections”.

There are two release paths:

#### `release_connection(conn)`

Fast path. This assumes the connection is already idle and fully drained.

If the connection:

* is still valid (`connected() == true`), and
* appears idle (no pending work),

then it is pushed back into the `idle_` queue for reuse.

If not, the pool instead **drops** the connection:

* It does not return it to `idle_`.
* `live_count_` is decremented.
* When the last reference to that connection dies, the destructor closes the socket and calls `PQfinish`.

In other words: a “dirty” (not-idle) connection is destroyed instead of being recycled.

This guarantees that no coroutine will ever receive a still-busy `PGconn`.

#### `release_connection_async(conn)`

This variant is awaitable and is used internally by `PgTransaction` when returning a connection.

It actively drains the connection before recycling:

* It pumps input from the PG socket.
* It consumes (and discards) all remaining `PGresult` frames.
* After that, it can safely requeue the connection.

This path is heavier but preserves the connection without killing it.

### Practical effect

* You never see `"another command is already in progress"` from normal usage (`query_awaitable`, transactions, etc.).
* The pool self-heals: if a connection comes back still mid-protocol, it’s simply retired and replaced with a new one
  later.
* Long-running operations like `COPY` and cursor streaming don’t poison the pool.

---

## Manual connection usage

You can explicitly take a connection, do multiple steps, and then release it.

```cpp
task::Awaitable<void> manual_usage()
{
    auto& pool = usub::pg::PgPool::instance();
    auto conn = co_await pool.acquire_connection();

    if (!conn || !conn->connected())
    {
        std::cout << "[ERROR] no connection\n";
        co_return;
    }

    auto res = co_await pool.query_on(conn, "SELECT now();");
    if (res.ok && !res.rows.empty())
        std::cout << "now() = " << res.rows[0].cols[0] << "\n";

    pool.release_connection(conn); // may recycle or retire the connection
    co_return;
}
```

For long-lived listeners (`LISTEN ...`/`NOTIFY`) or background tasks, you typically **do not** release them back to the
pool; you “own” that connection and keep it dedicated.

---

## High-level query API

For most workloads, use `query_awaitable()`:

```cpp
auto res = co_await pool.query_awaitable(
    "UPDATE users SET name = $1 WHERE id = $2 RETURNING name;",
    "John", 1
);
```

This convenience layer:

* acquires a connection,
* executes parameterized or simple query,
* releases/recovers the connection (including idle safety logic).

---

## Bulk COPY

The pool exposes COPY via `PgConnectionLibpq`, not directly on `PgPool`. Typical usage:

```cpp
task::Awaitable<void> bulk_insert_example()
{
    auto& pool = usub::pg::PgPool::instance();
    auto conn = co_await pool.acquire_connection();

    auto begin_copy = co_await conn->copy_in_start(
        "COPY public.bigdata(payload) FROM STDIN"
    );

    if (!begin_copy.ok)
    {
        std::cout << "[ERROR] COPY IN start failed: " << begin_copy.error << "\n";
        pool.release_connection(conn);
        co_return;
    }

    // stream rows
    for (int i = 0; i < 5; i++)
    {
        std::string line = "payload line " + std::to_string(i) + "\n";
        auto chunk_res = co_await conn->copy_in_send_chunk(
            line.data(), line.size()
        );
        if (!chunk_res.ok)
        {
            std::cout << "[ERROR] COPY IN chunk failed: " << chunk_res.error << "\n";
            pool.release_connection(conn);
            co_return;
        }
    }

    auto fin = co_await conn->copy_in_finish();
    if (!fin.ok)
    {
        std::cout << "[ERROR] COPY IN finish failed: " << fin.error << "\n";
        pool.release_connection(conn);
        co_return;
    }

    std::cout << "[INFO] COPY IN done, rows_affected=" << fin.rows_affected << "\n";

    pool.release_connection(conn);
    co_return;
}
```

`PgCopyResult` is described in `results.md`.

There is also COPY OUT streaming:

```cpp
auto st = co_await conn->copy_out_start(
    "COPY (SELECT id, payload FROM public.bigdata ORDER BY id) TO STDOUT"
);

if (!st.ok) { /* handle error */ }

while (true)
{
    auto chunk = co_await conn->copy_out_read_chunk();
    if (!chunk.ok)
    {
        std::cout << "[ERROR] COPY OUT read failed: " << chunk.err.message << "\n";
        break;
    }

    if (chunk.value.empty())
    {
        std::cout << "[INFO] COPY OUT finished\n";
        break;
    }

    std::string s(chunk.value.begin(), chunk.value.end());
    std::cout << "[COPY-OUT-CHUNK] " << s;
}
```

---

## Server-side cursors (chunked fetch)

For large result sets, you can stream rows without loading everything into memory.

```cpp
task::Awaitable<void> cursor_stream_example()
{
    auto& pool = usub::pg::PgPool::instance();
    auto conn = co_await pool.acquire_connection();

    std::string cursor_name = conn->make_cursor_name();

    // DECLARE cursor inside a transaction (BEGIN; DECLARE ...;)
    auto decl_res = co_await conn->cursor_declare(
        cursor_name,
        "SELECT id, payload FROM public.bigdata ORDER BY id"
    );
    if (!decl_res.ok)
    {
        std::cout << "[ERROR] cursor DECLARE failed: " << decl_res.error << "\n";
        pool.release_connection(conn);
        co_return;
    }

    for (;;)
    {
        usub::pg::PgCursorChunk chunk =
            co_await conn->cursor_fetch_chunk(cursor_name, 3);

        if (!chunk.ok)
        {
            std::cout << "[ERROR] cursor FETCH failed: " << chunk.error << "\n";
            break;
        }

        if (chunk.rows.empty())
        {
            std::cout << "[INFO] cursor exhausted\n";
            break;
        }

        for (auto& row : chunk.rows)
        {
            std::cout << "[CURSOR] id=" << row.cols[0]
                      << " payload=" << row.cols[1] << "\n";
        }

        if (chunk.done)
            break;
    }

    auto close_res = co_await conn->cursor_close(cursor_name);
    if (!close_res.ok)
    {
        std::cout << "[WARN] cursor close failed: " << close_res.error << "\n";
    }

    pool.release_connection(conn);
    co_return;
}
```

`PgCursorChunk` is described in `results.md`.

This approach:

* Opens a cursor (`DECLARE ... CURSOR FOR <query>`)
* Fetches N rows at a time using `FETCH FORWARD <N> FROM <cursor>`
* Closes cursor and commits

The pool’s recycling logic ensures that after this multi-step usage, the connection is either safely recycled (if
drained) or retired and replaced.

---

## Queue capacity

`queue_capacity` limits how many idle connections the pool will keep.

* If the idle queue is full when a connection is released, that connection is dropped (socket closed, `PQfinish`).
* When load increases again, new connections are created on demand.

This prevents unbounded idle socket buildup.

---

## Summary

| Feature                   | Description                                                   |
|---------------------------|---------------------------------------------------------------|
| Fully async               | All waits are coroutine suspension, not blocking threads      |
| Safe pooling              | Lock-free MPMC queue for idle connections                     |
| Dirty-connection handling | Busy/undrained connections are killed or drained before reuse |
| COPY streaming            | High-volume ingest/export without buffering whole dataset     |
| Cursor chunking           | Controlled incremental fetch from large queries               |
| Health checker            | Background liveness / reachability monitor                    |
| Structured errors         | All operations return rich error objects (no exceptions)      |