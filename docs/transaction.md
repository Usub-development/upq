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

You can specify isolation and access modes through `PgTransactionConfig`:

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

Supported isolation levels:

* `READ COMMITTED`
* `REPEATABLE READ`
* `SERIALIZABLE`

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

If the connection is lost mid-transaction:

* `qr.ok == false`
* `qr.code == PgErrorCode::ConnectionClosed`
* Transaction state automatically becomes inactive + rolled back.

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

Additional helpers:

* `co_await txn.abort();` → sends `ABORT`
* `co_await txn.finish();` → rollback if still active, then cleanup

---

## Connection lifecycle

Each `PgTransaction` pins one connection:

1. Acquired from pool with `acquire_connection()`
2. Used exclusively for this transaction
3. Released using `release_connection_async()` after commit/rollback

`release_connection_async()` drains all remaining results, ensuring no dirty state before returning the connection to
the pool.

If the connection is broken, it’s retired instead of recycled.

This guarantees:

* No `"another command is already in progress"`
* No leaking results between coroutines

---

## Subtransactions (SAVEPOINT)

Nested transactions are supported via `PgSubtransaction`.

```cpp
auto sub = txn.make_subtx();

if (co_await sub.begin())
{
    auto r = co_await sub.query("UPDATE t SET v=$1 WHERE id=$2", 42, 5);
    if (!r.ok)
        co_await sub.rollback();
    else
        co_await sub.commit();
}
```

Semantics:

| Method       | SQL issued                     |
|--------------|--------------------------------|
| `begin()`    | `SAVEPOINT <name>`             |
| `commit()`   | `RELEASE SAVEPOINT <name>`     |
| `rollback()` | `ROLLBACK TO SAVEPOINT <name>` |

They share the same connection as the parent transaction.
If a subtransaction fails, the main transaction remains active unless you roll it back explicitly.

---

## Error model

* No exceptions — all results are structured (`QueryResult`)
* Connection failures automatically mark the transaction as rolled back
* Invalid usage (calling `query()` after rollback) yields `PgErrorCode::InvalidFuture`

---

## Summary

| Feature                | Description                                      |
|------------------------|--------------------------------------------------|
| Dedicated connection   | One `PGconn` per transaction                     |
| Configurable isolation | Serializable / Repeatable Read / Read Committed  |
| Async operations       | All calls suspend coroutine, never block threads |
| Safe pool return       | Uses `release_connection_async` for cleanup      |
| Auto invalidation      | Broken connections mark transaction rolled back  |
| Subtransactions        | Nested `SAVEPOINT` support                       |
| Structured errors      | Always returns `QueryResult`                     |