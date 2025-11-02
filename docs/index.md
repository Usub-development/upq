# upq

`upq` is an asynchronous PostgreSQL client and connection pool for modern C++23.  
It’s designed for **uvent**, built around coroutines, and avoids libpqxx and blocking calls entirely.

## Design Goals

- Fully non-blocking I/O using `uvent`
- No extra runtime layers or threads
- Coroutine-based queries
- Clean separation between pool, connection, and transaction
- Minimal allocations and zero unnecessary copies
- Opt-in reflection for ergonomic reads/writes (zero boilerplate, positional mapping)

## Components

| Component             | Purpose                                                   |
|-----------------------|-----------------------------------------------------------|
| **PgPool**            | Global connection pool, manages `PGconn` instances        |
| **PgConnectionLibpq** | Async low-level PostgreSQL connection wrapper             |
| **PgTransaction**     | Transaction wrapper built on pooled connections           |
| **QueryResult**       | Lightweight query result container                        |
| **PgReflect**         | Header-only reflect helpers and mappers (positional only) |

## Features

- **Reflect-aware SELECT** → `std::vector<T>` / `std::optional<T>`  
  Positional mapping: column order in `SELECT` must match the field order of `T` (or tuple elements).
- **Reflect-aware params** for `INSERT/UPDATE`:
    - Aggregates/tuples are expanded into multiple `$1..$N` parameters.
    - Containers (`std::vector`, `std::array`, `T[N]`, `initializer_list`) are sent as a single typed PostgreSQL array
      parameter.
    - `std::optional<T>` maps to `NULL` or a value.
- Stays close to libpq semantics; no hidden background threads, no magic.

**Tiny example**

```cpp
struct User { int64_t id; std::string name; std::optional<std::string> password; };

// Read many
auto users = co_await pool.query_reflect<User>(
    "SELECT id, name, password FROM users ORDER BY id LIMIT 100"
);

// Insert from aggregate
struct NewUser { std::string name; std::optional<std::string> password; std::vector<int> roles; };
NewUser nu{"alice", std::nullopt, {1,2}};
co_await pool.exec_reflect(
    "INSERT INTO users(name, password, roles) VALUES ($1,$2,$3)",
    nu
);
```

## What it *doesn’t* do

* No ORM
* No query builders or migration tools
* No automatic reconnection or retry logic
* No external dependencies besides `libpq` and `uvent`

The philosophy: **use coroutines, keep it minimal, and let the compiler inline everything**.