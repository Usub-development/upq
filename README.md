# 🧩 upq: Asynchronous PostgreSQL integration for uvent

### Overview

`upq` is a coroutine-based, fully asynchronous PostgreSQL driver built on top of **libpq** and integrated directly into
the **uvent** event loop.
It provides a non-blocking query interface, connection pooling, parameterized statements, and RAII-based transactional
wrappers — all without any external async libraries.

---

### ✳️ Features

* Asynchronous connect using `PQconnectStart` + `PQconnectPoll` integrated with `uvent`’s event system
* Coroutine-awaitable I/O (`co_await pool->query_awaitable(...)`)
* Global static connection pool (`PgPool::instance()`)
* Connection reuse with lock-free MPMC queue
* Safe transactional RAII wrapper (`PgTransaction`)
* Parameter binding with `$1, $2, …` syntax
* Zero external dependencies beyond **libpq** and **spdlog**

---

### ⚙️ Example Usage

```cpp
#include "PgPool.h"
#include "uvent/Uvent.h"

using namespace usub::uvent;

task::Awaitable<void> test_db_query()
{
    // Ensure table exists
    co_await usub::pg::PgPool::instance().query_awaitable(
        "CREATE TABLE IF NOT EXISTS public.users("
        "id SERIAL PRIMARY KEY,"
        "name TEXT,"
        "password TEXT);"
    );

    // Transaction block with parameterized query
    {
        usub::pg::PgTransaction txn;
        co_await txn.begin();

        auto upd = co_await txn.query(
            "UPDATE users SET name=$1 WHERE id=$2 RETURNING name;",
            "John", 1
        );

        if (upd.ok && !upd.rows.empty())
            spdlog::info("Updated name={}", upd.rows[0].cols[0]);

        co_await txn.commit();
    }

    // Regular SELECT outside of transaction
    auto res = co_await usub::pg::PgPool::instance().query_awaitable(
        "SELECT id, name FROM users ORDER BY id LIMIT $1;", 5
    );

    if (res.ok)
        for (auto& r : res.rows)
            spdlog::info("user: id={}, name={}", r.cols[0], r.cols[1]);

    co_return;
}

int main()
{
    usub::Uvent uvent(4);

    // Initialize global connection pool
    usub::pg::PgPool::init_global("localhost", "12432",
                                  "postgres", "postgres", "password");

    // Spawn coroutine test
    system::co_spawn_static(test_db_query(), 0);

    uvent.run();
}
```

---

### 🔩 Components

| Component           | Description                                                                           |
|---------------------|---------------------------------------------------------------------------------------|
| `PgPool`            | Global async connection pool built on `libpq` and `uvent::MPMCQueue`.                 |
| `PgConnectionLibpq` | Wraps one `PGconn`, handles `PQconnectPoll`, `PQsendQuery`, `PQconsumeInput`.         |
| `PgTransaction`     | RAII wrapper for BEGIN / COMMIT / ROLLBACK; automatically returns connection to pool. |
| `QueryResult`       | Simple struct containing `rows`, `cols`, `ok`, and `error`.                           |

---

### 🧠 Design Notes

* All I/O operations are coroutine-friendly (`co_await` on read/write readiness).
* No blocking calls — even `connect()` and `flush()` are async via uvent awaiters.
* The pool automatically grows and reuses connections.
* Thread-safe access: multiple threads can concurrently `acquire_connection()` and `release_connection()`.

---

### 🧱 Example Schema (minified)

```sql
CREATE TABLE IF NOT EXISTS public.users
(
    id
    SERIAL
    PRIMARY
    KEY,
    name
    TEXT,
    password
    TEXT
);
```

---

### 🚀 Build

Requires:

* C++23 compiler (clang ≥16 / gcc ≥13)
* `libpq` (PostgreSQL client library)
* uvent headers

Example CMake:

```cmake
find_package(PostgreSQL REQUIRED)
target_link_libraries(app PRIVATE PostgreSQL::PostgreSQL spdlog::spdlog)
```

---

### 📜 License

MIT License © Usub-development