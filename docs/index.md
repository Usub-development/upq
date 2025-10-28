# upq

`upq` is an asynchronous PostgreSQL client and connection pool for modern C++23.  
It’s designed for **uvent**, built around coroutines, and avoids libpqxx and blocking calls entirely.

## Design Goals

- Fully non-blocking I/O using `uvent`
- No extra runtime layers or threads
- Coroutine-based queries
- Clean separation between pool, connection, and transaction
- Minimal allocations and zero unnecessary copies

## Components

| Component             | Purpose                                            |
|-----------------------|----------------------------------------------------|
| **PgPool**            | Global connection pool, manages `PGconn` instances |
| **PgConnectionLibpq** | Async low-level PostgreSQL connection wrapper      |
| **PgTransaction**     | Transaction wrapper built on pooled connections    |
| **QueryResult**       | Lightweight query result container                 |

## What it *doesn’t* do

- No ORM or reflection mapping
- No query builders or migration tools
- No automatic reconnection or retry logic
- No external dependencies besides `libpq` and `uvent`

The philosophy: **use coroutines, keep it minimal, and let the compiler inline everything**.
