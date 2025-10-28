# Quickstart

## 1. Initialize the global pool

Initialize once during your application bootstrap:

```cpp
#include "upq/PgPool.h"

void init_db()
{
    usub::pg::PgPool::init_global(
        "localhost", // host
        "12432",         // port
        "postgres",      // user
        "mydb",          // database
        "password",      // password
        32,              // max pool size
        64               // queue capacity
    );
}
```

Then access the singleton anywhere via:

```cpp
auto& pool = usub::pg::PgPool::instance();
```

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
        "password TEXT);"
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

Each `row.cols[i]` is a std::string. `NULL` values become empty strings.