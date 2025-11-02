# PgTransaction

`PgTransaction` provides coroutine-safe transactional operations for PostgreSQL using a dedicated pooled connection.

It wraps `BEGIN` / `COMMIT` / `ROLLBACK` logic, automatically manages connection reuse, and supports nested
subtransactions (`SAVEPOINT`).

---

## Overview

- Uses a **dedicated connection** from `PgPool`
- Sends `BEGIN` with configurable isolation / read-only / deferrable
- Executes any number of async queries
- Commits or rolls back explicitly
- Returns or retires the connection automatically
- Supports **subtransactions** via `SAVEPOINT`
- Supports **pipelined query execution** via compile-time toggle
- **Reflect-aware helpers** for SELECT and param binding

All methods are asynchronous and return `Awaitable`.

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

Specify isolation and access modes via `PgTransactionConfig`:

```cpp
usub::pg::PgTransactionConfig cfg{
    .isolation  = usub::pg::TxIsolationLevel::Serializable,
    .read_only  = false,
    .deferrable = false
};

usub::pg::PgTransaction txn(&usub::pg::PgPool::instance(), cfg);
co_await txn.begin();
```

Generated SQL for `BEGIN`:

```
BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE;
```

or, for example:

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

Reflection provides struct/tuple mapping for results and positional expansion for parameters.

### SELECT → `std::vector<T>` / `std::optional<T>`

```cpp
struct UserRow
{
    int64_t id;
    std::string name;
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

auto many = co_await txn.select_reflect<UserRow>(
    "SELECT id, name, password, roles, tags FROM users ORDER BY id"
);

auto one = co_await txn.select_one_reflect<UserRow>(
    "SELECT id, name, password, roles, tags FROM users WHERE id = 1"
);
```

### Aggregate/tuple → parameters (`INSERT/UPDATE`)

```cpp
struct NewUser
{
    std::string name;
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

NewUser nu{ "bob", std::nullopt, {1, 2}, {"vip"} };

auto ins = co_await txn.query_reflect(
    "INSERT INTO users(name, password, roles, tags) VALUES ($1,$2,$3,$4)",
    nu
);
```

**Rules**

* Positional mapping: column order in `SELECT` must match member order in `T` (or tuple elements).
* `std::optional<T>` ↔ `NULL`/value.
* Containers (`std::vector`, `std::array`, `T[N]`, `initializer_list`) are sent as a single typed PostgreSQL array
  parameter.
* Aggregates/tuples expand into multiple `$1..$N`.
* Pointers (except `char*`) are not supported as parameters.

---

### Pipelined query execution

Enable pipeline mode at compile time using a template flag `<true>`.

```cpp
auto r1 = co_await txn.query<true>("INSERT INTO logs(message) VALUES($1)", "A");
auto r2 = co_await txn.query<true>("INSERT INTO logs(message) VALUES($1)", "B");
```

When `<true>`:

* Commands queue without waiting for individual results.
* Driver sends `PQsendQueryParams` back-to-back and flushes once.
* Results are collected in order after an implicit sync.

Use only for **independent** statements. `<false>` (default) executes sequentially.

| Template flag | Description                                         |
|---------------|-----------------------------------------------------|
| `<false>`     | Default. Sequential per-query execution (safe).     |
| `<true>`      | Pipeline mode. Batched sends with deferred results. |

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

Each `PgTransaction` pins one connection:

1. Acquired from pool with `acquire_connection()`
2. Used exclusively for this transaction
3. Released using `release_connection_async()` after commit/rollback

`release_connection_async()` drains remaining results to avoid dirty state.
Broken connections are retired.

---

## Subtransactions (SAVEPOINT)

Nested transactions via `PgSubtransaction`.

```cpp
auto sub = txn.make_subtx();

if (co_await sub.begin())
{
    // regular
    auto r = co_await sub.query("UPDATE t SET v=$1 WHERE id=$2", 42, 5);

    // reflect params
    struct Patch { int v; int id; };
    r = co_await sub.query_reflect("UPDATE t SET v=$1 WHERE id=$2", Patch{42, 5});

    // reflect select
    struct Row { int id; int v; };
    auto got = co_await sub.select_one_reflect<Row>("SELECT id, v FROM t WHERE id=5");

    if (!r.ok) co_await sub.rollback();
    else       co_await sub.commit();
}
```

Semantics:

| Method       | SQL issued                     |
|--------------|--------------------------------|
| `begin()`    | `SAVEPOINT <name>`             |
| `commit()`   | `RELEASE SAVEPOINT <name>`     |
| `rollback()` | `ROLLBACK TO SAVEPOINT <name>` |

They share the parent connection. A failed subtransaction does not auto-rollback the parent.

---

## Error model

* No exceptions — all results are structured (`QueryResult`)
* Connection failures mark the transaction rolled back
* Invalid usage (e.g., `query()` after rollback) yields `PgErrorCode::InvalidFuture`

---

## Summary

| Feature                | Description                                             |
|------------------------|---------------------------------------------------------|
| Dedicated connection   | One `PGconn` per transaction                            |
| Configurable isolation | Serializable / Repeatable Read / Read Committed         |
| Async operations       | Coroutine suspension only                               |
| Safe pool return       | `release_connection_async` drains before recycle        |
| Auto invalidation      | Broken connections are retired                          |
| Subtransactions        | Nested `SAVEPOINT` support                              |
| **Reflect helpers**    | `select_reflect`, `select_one_reflect`, `query_reflect` |
| Pipeline execution     | Compile-time toggle `<true>` for batched queries        |