# PgPool

`PgPool` manages a set of live PostgreSQL connections (`PGconn` + non-blocking TCP socket).  
It distributes connections among coroutines and automatically recycles them.

---

## Initialization

```cpp
usub::pg::PgPool::init_global(
    host, port, user, db, password,
    max_pool_size, queue_capacity
);
```

Then:

```cpp
auto& pool = usub::pg::PgPool::instance();
```

## Manual connection control

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

Useful for:
- running multiple sequential queries on one connection 
- implementing custom LISTEN/NOTIFY listeners

## Simplified API
Typical usage:
```cpp
auto res = co_await pool.query_awaitable(
    "UPDATE users SET name = $1 WHERE id = $2 RETURNING name;",
    "John", 1
);
```

`query_awaitable()` automatically:
- Acquires a connection
- Executes query
- Releases it back to the pool