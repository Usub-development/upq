# PgPool

`PgPool` is an asynchronous PostgreSQL connection pool.  
It manages live `PGconn` sockets, hands them out to coroutines, and safely recycles (or retires) them.

Primary high-level entrypoint for queries in **upq**.

---

## Overview

`PgPool` provides:

- Bounded pooling via `max_pool_size`
- Non-blocking acquisition (`co_await` suspends coroutines, not threads)
- Safe recycling with dirty-connection handling
- Structured errors (`QueryResult`)
- Optional periodic health checks (`PgPoolHealthConfig`)
- **Reflective parameter/result mapping** (`query_reflect`, `exec_reflect`, `query_reflect_one`)
- Full compatibility with low-level features on `PgConnectionLibpq`:
    - `COPY ... FROM STDIN` / `COPY ... TO STDOUT`
    - Server-side cursors with chunked fetch

---

## Construction

```cpp
usub::pg::PgPool pool{
    /*host*/     "127.0.0.1",
    /*port*/     "5432",
    /*user*/     "postgres",
    /*db*/       "app",
    /*password*/ "secret",
    /*max_pool*/ 32,
    /*health*/   usub::pg::PgPoolHealthConfig{
        .enabled = true,
        .interval_ms = 3'000
    }
};
```

Health checker is optional; when enabled, it runs inside the event loop.

Accessors:

```cpp
pool.host(); pool.port(); pool.user(); pool.db(); pool.password();
pool.health_cfg();          // returns current health config (by value)
pool.health_checker();      // reference to checker object
pool.health_stats();        // counters (see below)
```

---

## Health checker

Runs periodically and validates connectivity using lightweight probes.
It updates pool-level stats:

```cpp
struct HealthStats {
    std::atomic<uint64_t> checked;     // total checks performed
    std::atomic<uint64_t> alive;       // successful checks
    std::atomic<uint64_t> reconnected; // connections that had to be re-opened
};
```

Read stats:

```cpp
auto& hs = pool.health_stats();
std::cout << "checked=" << hs.checked
          << " alive=" << hs.alive
          << " reconnected=" << hs.reconnected << "\n";
```

Default interval: `600000 ms` (10 minutes).

---

## Connection lifecycle

```cpp
auto conn = co_await pool.acquire_connection();
// use conn...
pool.release_connection(conn);            // fast path
// or:
co_await pool.release_connection_async(conn); // drain & recycle
```

### Dirty-connection handling

Libpq allows only one in-flight command per connection. If a connection returns
with unread results/pending input:

* `release_connection(conn)` detects it and **retires** the connection (not put
  back to idle; closed when refs drop; pool live counter decreases).
* `release_connection_async(conn)` actively **drains** the socket (consumes
  pending `PGresult`s) and then recycles safely.

This prevents `"another command is already in progress"` from leaking to user code.

### Marking a connection dead

If you detect a terminal condition, tell the pool:

```cpp
pool.mark_dead(conn); // retires it; do not reuse
```

---

## High-level query API

Use `query_awaitable()` for one-shot work:

```cpp
auto res = co_await pool.query_awaitable(
    "UPDATE users SET name=$1 WHERE id=$2 RETURNING name;",
    "John", 1
);

if (res.ok) {
    // res.rows[0].cols[0] == "John"
}
```

Or bind to a specific connection:

```cpp
auto conn = co_await pool.acquire_connection();
auto res  = co_await pool.query_on(conn, "SELECT now()");
pool.release_connection(conn);
```

Both APIs return `QueryResult` with `ok`, `code`, `error`, `rows`, `rows_valid`.

---

## Reflect-aware API

Reflection-based helpers provide **automatic mapping between SQL and C++ aggregates**.
Powered by [ureflect](https://github.com/Usub-development/ureflect), they support structured parameter binding and row
decoding.

### SELECT → `std::vector<T>` or `std::optional<T>`

```cpp
struct UserRow {
    int64_t id;
    std::string username;
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

// Multiple rows
auto users = co_await pool.query_reflect<UserRow>(
    "SELECT id, name AS username, password, roles, tags FROM users ORDER BY id;"
);

// Single row
auto one = co_await pool.query_reflect_one<UserRow>(
    "SELECT id, name AS username, password, roles, tags FROM users WHERE id = $1;",
    1
);
```

### INSERT/UPDATE from aggregates or tuples

```cpp
struct NewUser {
    std::string name;
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

NewUser nu{"bob", std::nullopt, {1, 2}, {"vip"}};

auto r1 = co_await pool.exec_reflect(
    "INSERT INTO users(name,password,roles,tags) VALUES ($1,$2,$3,$4);",
    nu
);

// using tuple
std::tuple<std::string, std::optional<std::string>, std::vector<int>, std::vector<std::string>> tup{
    "alice", "pw", {3,4}, {"dev","core"}
};
auto r2 = co_await pool.exec_reflect(
    "INSERT INTO users(name,password,roles,tags) VALUES ($1,$2,$3,$4);",
    tup
);
```

### Mapping rules

* Fields matched **by name** (aliases supported: `AS username`)
* `std::optional<T>` ↔ `NULL`
* Containers (`vector`, `array`, etc.) ↔ PostgreSQL arrays
* Aggregates and tuples expand to `$1..$N` parameters
* Non-aggregate containers are passed as array parameters
* Fully compile-time; no runtime reflection

### API summary

| Method                               | Description                                              |
|--------------------------------------|----------------------------------------------------------|
| `query_reflect<T>(sql, ...)`         | SELECT → `std::vector<T>` with optional bound parameters |
| `query_reflect_one<T>(sql, ...)`     | SELECT → `std::optional<T>` (single row)                 |
| `exec_reflect(sql, obj)`             | Executes using struct or tuple fields as parameters      |
| `query_on_reflect<T>(conn, sql)`     | Same as above, bound to connection                       |
| `query_on_reflect_one<T>(conn, sql)` | Single-row variant                                       |
| `exec_reflect_on(conn, sql, obj)`    | Aggregate parameter execution on given connection        |

---

## Bulk COPY (via `PgConnectionLibpq`)

```cpp
auto conn = co_await pool.acquire_connection();

auto st = co_await conn->copy_in_start("COPY public.items(v) FROM STDIN");
if (!st.ok) { pool.release_connection(conn); co_return; }

for (int i = 0; i < 5; i++) {
    std::string line = std::to_string(i) + "\n";
    auto r = co_await conn->copy_in_send_chunk(line.data(), line.size());
    if (!r.ok) { pool.release_connection(conn); co_return; }
}

auto fin = co_await conn->copy_in_finish(); // PgCopyResult
pool.release_connection(conn);
```

`COPY TO STDOUT` is analogous with `copy_out_start()` / `copy_out_read_chunk()`.

---

## Server-side cursors (chunked fetch)

For large result sets, stream rows without loading all into memory.

```cpp
task::Awaitable<void> cursor_stream_example()
{
    auto& pool = usub::pg::PgPool::instance();
    auto conn = co_await pool.acquire_connection();

    std::string cursor_name = conn->make_cursor_name();

    auto decl_res = co_await conn->cursor_declare(
        cursor_name,
        "SELECT id, payload FROM public.bigdata ORDER BY id"
    );
    if (!decl_res.ok) { pool.release_connection(conn); co_return; }

    for (;;)
    {
        usub::pg::PgCursorChunk chunk =
            co_await conn->cursor_fetch_chunk(cursor_name, 3);

        if (!chunk.ok || chunk.rows.empty()) break;

        for (auto& row : chunk.rows)
            std::cout << "[CURSOR] id=" << row.cols[0]
                      << " payload=" << row.cols[1] << "\n";

        if (chunk.done) break;
    }

    auto close_res = co_await conn->cursor_close(cursor_name);
    if (!close_res.ok)
        std::cout << "[WARN] cursor close failed: " << close_res.error << "\n";

    pool.release_connection(conn);
    co_return;
}
```

---

## Error model

* No exceptions — all structured results.
* On invalid/closed connection, `query_on()` returns:

```cpp
ok = false
code = PgErrorCode::ConnectionClosed
error = "connection invalid"
rows_valid = false
```

Pool self-heals by retiring broken connections and reopening as needed.

---

## Summary

| Feature              | Description                                                     |
|----------------------|-----------------------------------------------------------------|
| Fully async          | Coroutine suspension only; no blocking threads                  |
| Bounded pooling      | `max_pool_size`                                                 |
| Lock-free idle queue | MPMC queue of `shared_ptr<PgConnectionLibpq>`                   |
| Dirty handling       | Drain on async release, otherwise retire                        |
| Health checks        | Optional periodic probes with `checked/alive/reconnected` stats |
| COPY & cursors       | Streaming-safe, via `PgConnectionLibpq`                         |
| Reflect API          | Struct/tuple ↔ SQL mapping (`query_reflect`, `exec_reflect`)    |
| Structured errors    | Clear, exception-free failure reporting                         |