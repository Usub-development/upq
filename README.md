# 🧩 upq: Asynchronous PostgreSQL integration for uvent

### Overview

`upq` is a coroutine-based, fully asynchronous PostgreSQL client built on top of **libpq** and integrated directly into
the **uvent** event loop.  
It provides a non-blocking query interface, connection pooling, parameterized statements, RAII-based transactions,  
and now — full reflection-based mapping with [ureflect](https://github.com/Usub-development/ureflect).

---

### ✳️ Features

* Asynchronous connect via `PQconnectStart` + `PQconnectPoll` integrated with `uvent`
* Coroutine-awaitable queries (`co_await pool.query_awaitable(...)`)
* Global static connection pool (`PgPool::instance()`)
* Lock-free MPMC queue for connection reuse
* Safe transactional RAII wrapper (`PgTransaction`)
* Parameter binding with `$1, $2, …`
* Zero-copy pipeline, no async wrappers
* **NEW:** Reflection support — map structs ↔ SQL rows automatically

---

### ⚙️ Example Usage

```cpp
#include "PgPool.h"
#include "uvent/Uvent.h"

using namespace usub::uvent;

task::Awaitable<void> test_db_query()
{
    // Create schema
    co_await usub::pg::PgPool::instance().query_awaitable(R"SQL(
        CREATE TABLE IF NOT EXISTS public.users(
            id SERIAL PRIMARY KEY,
            name TEXT,
            password TEXT
        );
    )SQL");

    // Transaction example
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

    // Regular SELECT outside transaction
    auto res = co_await usub::pg::PgPool::instance().query_awaitable(
        "SELECT id, name FROM users ORDER BY id LIMIT $1;", 5
    );

    if (res.ok)
        for (auto& r : res.rows)
            spdlog::info("user: id={}, name={}", r.cols[0], r.cols[1]);

    co_return;
}
````

---

### 🧩 Reflect Integration (v2.0.0)

`upq` integrates directly with `ureflect` — allowing you to pass and receive typed C++ structures without manual
mapping.

```cpp
struct User {
    int64_t id;
    std::string name;
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

// Insert from struct
User u{"Alice", std::nullopt, {1, 2, 5}, {"admin", "core"}};
co_await pool.exec_reflect(
    "INSERT INTO users(name, password, roles, tags) VALUES($1,$2,$3,$4);", u
);

// Fetch directly into vector<User>
auto rows = co_await pool.query_reflect<User>(
    "SELECT id, name, password, roles, tags FROM users;"
);

for (auto& r : rows)
    spdlog::info("User {}: roles={}, tags={}", r.name, r.roles.size(), r.tags.size());
```

**Key notes:**

* Struct fields are matched to columns **by name**.
* `std::optional<T>` ↔ SQL `NULL`
* `std::vector<T>` ↔ PostgreSQL array
* Reflection functions are header-only, zero-overhead

---

### 🔩 Components

| Component           | Description                                                                           |
|---------------------|---------------------------------------------------------------------------------------|
| `PgPool`            | Global async connection pool built on `libpq` and `uvent::MPMCQueue`.                 |
| `PgConnectionLibpq` | Wraps one `PGconn`, handles `PQconnectPoll`, `PQsendQuery`, `PQconsumeInput`.         |
| `PgTransaction`     | RAII wrapper for BEGIN / COMMIT / ROLLBACK; automatically returns connection to pool. |
| `PgReflect`         | Reflection-based parameter and result mapper using `ureflect`.                        |
| `QueryResult`       | Simple struct containing `rows`, `cols`, `ok`, and `error`.                           |

---

### 🧠 Design Notes

* All I/O operations are coroutine-friendly (`co_await` on readiness).
* No blocking calls — even `connect()` and `flush()` are async via uvent awaiters.
* Lock-free MPMC queue ensures high-throughput pooling.
* Safe across threads; no locks on query path.
* Reflect-mapping is **compile-time** and **header-only**.

---

### 🧱 Example Schema

```sql
CREATE TABLE IF NOT EXISTS public.users_reflect (
      id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL,
      password TEXT, roles INT4[] NOT NULL,
      tags TEXT [] NOT NULL
);

```

---

### 🚀 Build

Requires:

* C++23 compiler (clang ≥16 / gcc ≥13)
* `libpq`
* `uvent` headers

Example CMake:

```cmake
include(FetchContent)

FetchContent_Declare(
        upq
        GIT_REPOSITORY https://github.com/Usub-development/upq.git
        GIT_TAG main
        OVERRIDE_FIND_PACKAGE
)

FetchContent_MakeAvailable(upq)

target_link_libraries(${PROJECT_NAME} PRIVATE
        usub::upq
)
```

---

### 📜 License

MIT License © Usub-development