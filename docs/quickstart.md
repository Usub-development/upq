# Quickstart

## 1. Initialize the global pool

Initialize once during your application bootstrap:

```cpp
#include "upq/PgPool.h"

void init_db()
{
    usub::pg::PgPool::init_global(
        "localhost",   // host
        "12432",       // port
        "postgres",    // user
        "mydb",        // database
        "password",    // password
        32,            // max pool size
        64             // queue capacity
    );
}
```

Then access the singleton anywhere via:

```cpp
auto& pool = usub::pg::PgPool::instance();
```

---

## 2. Basic query (no parameters)

```cpp
#include "uvent/Uvent.h"
#include "upq/PgPool.h"

using namespace usub::uvent;

task::Awaitable<void> create_schema()
{
    auto res = co_await usub::pg::PgPool::instance().query_awaitable(
        "CREATE TABLE IF NOT EXISTS users("
        "id SERIAL PRIMARY KEY,"
        "name TEXT,"
        "password TEXT,"
        "roles INT4[],"
        "tags  TEXT[]);"
    );

    if (!res.ok)
    {
        std::cout << "Schema init failed: " << res.error << "\n";
        co_return;
    }

    std::cout << "Schema created\n";
    co_return;
}
```

---

## 3. Query with parameters

```cpp
task::Awaitable<void> insert_user(std::string name, std::string pwd)
{
    auto& pool = usub::pg::PgPool::instance();

    auto res = co_await pool.query_awaitable(
        "INSERT INTO users (name, password) VALUES ($1, $2) RETURNING id;",
        name, pwd
    );

    if (!res.ok)
    {
        std::cout << "Insert failed: " << res.error << "\n";
        co_return;
    }

    if (!res.rows.empty())
        std::cout << "Inserted id = " << res.rows[0].cols[0] << "\n";

    co_return;
}
```

---

## 4. Reading query results

```cpp
task::Awaitable<void> get_user(int user_id)
{
    auto res = co_await usub::pg::PgPool::instance().query_awaitable(
        "SELECT id, name FROM users WHERE id = $1;",
        user_id
    );

    if (!res.ok)
    {
        std::cout << "Query failed: " << res.error << "\n";
        co_return;
    }

    for (auto& row : res.rows)
        std::cout << "id=" << row.cols[0] << " name=" << row.cols[1] << "\n";

    co_return;
}
```

---

## 5. Reflect-based queries (struct mapping)

Reflection provides automatic binding of aggregates/tuples to parameters
and automatic row → struct decoding. **Name-based mapping with aliases** is supported.

### SELECT → `std::vector<T>` / `std::optional<T>`

```cpp
struct UserRow
{
    int64_t id;
    std::string username;                   // maps from SQL alias: "name AS username"
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

task::Awaitable<void> get_all()
{
    auto& pool = usub::pg::PgPool::instance();

    auto rows = co_await pool.query_reflect<UserRow>(
        "SELECT id, name AS username, password, roles, tags FROM users ORDER BY id;"
    );

    for (auto& r : rows)
        std::cout << "user=" << r.username << "\n";
    co_return;
}

task::Awaitable<void> get_one(int64_t id)
{
    auto& pool = usub::pg::PgPool::instance();

    auto one = co_await pool.query_reflect_one<UserRow>(
        "SELECT id, name AS username, password, roles, tags FROM users WHERE id = $1;",
        id // parameters are supported with query_reflect(_one)
    );

    if (one)
        std::cout << "found " << one->username << "\n";
    co_return;
}
```

### Aggregate/tuple → parameters

```cpp
struct NewUser
{
    std::string name;
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

task::Awaitable<void> insert_user_reflect()
{
    auto& pool = usub::pg::PgPool::instance();

    NewUser u{ "bob", std::nullopt, {1, 2}, {"vip"} };
    auto r1 = co_await pool.exec_reflect(
        "INSERT INTO users(name, password, roles, tags) VALUES ($1,$2,$3,$4);",
        u
    );

    // tuple works too
    auto r2 = co_await pool.exec_reflect(
        "INSERT INTO users(name, password, roles, tags) VALUES ($1,$2,$3,$4);",
        std::tuple{ std::string{"alice"}, std::optional<std::string>{"x"}, std::vector<int>{3,4}, std::vector<std::string>{"dev","core"} }
    );

    (void)r1; (void)r2;
    co_return;
}
```

### Rules

* **Name-based** field matching; SQL aliases like `AS username` supported.
  If column names are unavailable, decoder falls back to positional order.
* `std::optional<T>` ↔ `NULL`.
* Standard containers (`vector`, fixed C-arrays, `initializer_list`) ↔ PostgreSQL arrays.
* Aggregates/tuples expand into `$1..$N` parameters.

---

## 6. Diagnostics

Every query returns structured error information:

```cpp
auto& pool = usub::pg::PgPool::instance();
auto res = co_await pool.query_awaitable("SELECT * FROM nonexistent;");
if (!res.ok) {
    std::cout
        << "Error: "   << res.error
        << " code="     << static_cast<uint32_t>(res.code)
        << " sqlstate=" << res.err_detail.sqlstate
        << " detail="   << res.err_detail.detail
        << " hint="     << res.err_detail.hint
        << "\n";
}
```

Use `rows_valid` to ensure result integrity before iterating.

---

## Field reference

| Field          | Description                                     |
|----------------|-------------------------------------------------|
| `ok`           | `true` if operation completed successfully      |
| `code`         | Structured category (`PgErrorCode`)             |
| `error`        | Human-readable message                          |
| `err_detail.*` | `{sqlstate, message, detail, hint}` from server |
| `rows_valid`   | `false` means row data incomplete or corrupted  |
| `columns`      | Column names (when available)                   |
| `rows`         | Result rows/columns as text slices              |

---

## Behavior summary

| Scenario           | ok | code               | Meaning                         |
|--------------------|----|--------------------|---------------------------------|
| Normal query       | ✅  | `OK`               | Operation succeeded             |
| Connection invalid | ❌  | `ConnectionClosed` | Socket or PGconn not usable     |
| Socket I/O error   | ❌  | `SocketReadFailed` | PQflush / PQconsumeInput failed |
| PostgreSQL error   | ❌  | `ServerError`      | SQLSTATE and diagnostics filled |
| Transaction misuse | ❌  | `InvalidFuture`    | Query on inactive transaction   |
| Unexpected issue   | ❌  | `Unknown`          | Fallback                        |

---

## Example

```cpp
auto res = co_await pool.query_awaitable("SELECT id, name FROM users;");
if (!res.ok) {
    std::cout
        << "Error: "   << res.error
        << " code="     << static_cast<uint32_t>(res.code)
        << " sqlstate=" << res.err_detail.sqlstate << "\n";
} else if (!res.rows_valid) {
    std::cout << "Data stream incomplete\n";
} else {
    for (auto& row : res.rows)
        std::cout << "id=" << row.cols[0] << " name=" << row.cols[1] << "\n";
}
```