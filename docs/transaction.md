# PgTransaction

`PgTransaction` provides coroutine-safe transactional operations for PostgreSQL using a dedicated pooled connection.

It wraps `BEGIN` / `COMMIT` / `ROLLBACK`, automatically manages connection reuse, supports nested subtransactions
(`SAVEPOINT`), and integrates reflection-based parameter/result mapping.

---

## Overview

- Uses a **dedicated connection** from `PgPool`
- Configurable isolation / read-only / deferrable modes
- Executes async queries on the same pinned connection
- Commits or rolls back explicitly
- Automatically returns or retires connection after use
- Supports **subtransactions** via `SAVEPOINT`
- Supports **pipelined execution** via compile-time flag
- **Reflect-aware helpers** for SELECT and parameter binding

All methods are coroutine-awaitable (`Awaitable`).

---

## Example

```cpp
task::Awaitable<void> transfer_example()
{
    usub::pg::PgTransaction txn;

    if (!co_await txn.begin())
    {
        std::cout << "[ERROR] BEGIN failed\n";
        co_return;
    }

    auto r1 = co_await txn.query(
        "UPDATE accounts SET balance = balance - $1 WHERE id = $2 RETURNING balance;",
        100, 1
    );

    auto r2 = co_await txn.query(
        "UPDATE accounts SET balance = balance + $1 WHERE id = $2 RETURNING balance;",
        100, 2
    );

    if (!r1.ok || !r2.ok)
    {
        co_await txn.rollback();
        co_return;
    }

    if (!co_await txn.commit())
    {
        std::cout << "[ERROR] COMMIT failed\n";
        co_return;
    }

    std::cout << "[OK] transfer complete\n";
}
```

---

## Configuration

Use `PgTransactionConfig` to control isolation and access mode:

```cpp
usub::pg::PgTransactionConfig cfg{
    .isolation  = usub::pg::TxIsolationLevel::Serializable,
    .read_only  = false,
    .deferrable = false
};

usub::pg::PgTransaction txn(&usub::pg::PgPool::instance(), cfg);
co_await txn.begin();
```

Generated SQL:

```
BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE;
```

Other examples:

```
BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY DEFERRABLE;
```

Supported isolation levels: `READ COMMITTED`, `REPEATABLE READ`, `SERIALIZABLE`.

---

## Executing queries

All queries run on the pinned connection:

```cpp
auto qr = co_await txn.query(
    "INSERT INTO logs(message) VALUES($1) RETURNING id;",
    "started"
);
if (!qr.ok)
{
    std::cout << "insert failed: " << qr.error << "\n";
    co_await txn.rollback();
}
```

---

## Reflect-based queries

Reflection enables automatic struct/tuple expansion for parameters and decoding of results into C++ aggregates.

### SELECT → `std::vector<T>` / `std::optional<T>`

```cpp
struct UserRow
{
    int64_t id;
    std::string username;                   // maps from "name AS username"
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

auto many = co_await txn.query_reflect<UserRow>(
    "SELECT id, name AS username, password, roles, tags FROM users ORDER BY id;"
);

auto one = co_await txn.query_reflect_one<UserRow>(
    "SELECT id, name AS username, password, roles, tags FROM users WHERE id = $1;",
    1
);
```

### Aggregates or tuples → parameters (`INSERT/UPDATE`)

```cpp
struct NewUser
{
    std::string name;
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

NewUser nu{ "bob", std::nullopt, {1, 2}, {"vip"} };

auto ins = co_await txn.exec_reflect(
    "INSERT INTO users(name, password, roles, tags) VALUES ($1,$2,$3,$4);",
    nu
);
```

### Mapping rules

* Fields matched **by name**; aliases (e.g. `AS username`) supported.
  If names unavailable, falls back to positional order.
* `std::optional<T>` ↔ `NULL`.
* Containers (`std::vector`, `std::array`, etc.) ↔ PostgreSQL arrays.
* Aggregates and tuples expand into `$1..$N` parameters.
* Pointers (except `char*`) are not supported.

---

## Pipelined query execution

Enable pipeline mode at compile time using template flag `<true>`:

```cpp
auto r1 = co_await txn.query<true>("INSERT INTO logs(msg) VALUES($1)", "A");
auto r2 = co_await txn.query<true>("INSERT INTO logs(msg) VALUES($1)", "B");
```

When `<true>`:

* Queries are queued without waiting for previous results.
* `PQsendQueryParams` calls are batched and flushed once.
* Results are consumed after an implicit sync.

Use only for **independent** statements.

| Template flag | Description                                    |
|---------------|------------------------------------------------|
| `<false>`     | Default. Sequential per-query execution.       |
| `<true>`      | Pipeline mode. Batched async query submission. |

---

## Commit and rollback

```cpp
bool ok = co_await txn.commit();
if (!ok)
    std::cout << "commit failed\n";
```

```cpp
co_await txn.rollback();
```

Helpers:

* `co_await txn.abort();` — sends `ABORT`
* `co_await txn.finish();` — rollback if active, then cleanup

---

## Connection lifecycle

Each transaction pins one connection:

1. Acquired from pool (`acquire_connection()`)
2. Used exclusively until `commit()` / `rollback()`
3. Released via `release_connection_async()`
   (which drains pending results before recycle)

Broken connections are retired automatically.

---

## Subtransactions (SAVEPOINT)

Nested transactions use `PgSubtransaction`.

```cpp
auto sub = txn.make_subtx();

if (co_await sub.begin())
{
    struct Patch { int v; int id; };
    auto upd = co_await sub.exec_reflect(
        "UPDATE t SET v=$1 WHERE id=$2",
        Patch{42, 5}
    );

    struct Row { int id; int v; };
    auto one = co_await sub.query_reflect_one<Row>(
        "SELECT id, v FROM t WHERE id=$1",
        5
    );

    if (!upd.ok) co_await sub.rollback();
    else         co_await sub.commit();
}
```

Semantics:

| Method       | SQL issued                     |
|--------------|--------------------------------|
| `begin()`    | `SAVEPOINT <name>`             |
| `commit()`   | `RELEASE SAVEPOINT <name>`     |
| `rollback()` | `ROLLBACK TO SAVEPOINT <name>` |

They share the same parent connection.
A failed subtransaction does not rollback the parent automatically.

---

## Error model

* No exceptions — all results are structured (`QueryResult`)
* Connection failures automatically invalidate the transaction
* Using after rollback yields `PgErrorCode::InvalidFuture`

---

## Summary

| Feature                | Description                                          |
|------------------------|------------------------------------------------------|
| Dedicated connection   | One `PGconn` per transaction                         |
| Configurable isolation | Serializable / Repeatable Read / Read Committed      |
| Async operations       | Coroutine-suspending only, non-blocking              |
| Safe pool return       | `release_connection_async()` drains before recycle   |
| Auto invalidation      | Broken connections retired automatically             |
| Subtransactions        | Nested `SAVEPOINT` support                           |
| Reflect helpers        | `query_reflect`, `query_reflect_one`, `exec_reflect` |
| Pipeline execution     | Compile-time `<true>` toggle for batched queries     |