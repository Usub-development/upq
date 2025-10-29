# PgTransaction

`PgTransaction` provides coroutine-friendly transactional access to PostgreSQL.

It:

- Acquires a dedicated connection from `PgPool`
- Sends `BEGIN` with optional isolation / read-only / deferrable settings
- Executes multiple queries on the same connection
- Commits or rolls back
- Returns (or retires) the connection back to the pool

All operations are async and return `Awaitable<...>`.

---

## Basic usage

```cpp
task::Awaitable<void> do_transfer()
{
    usub::pg::PgTransaction txn;

    bool ok_begin = co_await txn.begin();
    if (!ok_begin)
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
        std::cout << "[ERROR] transfer failed, rolling back\n";
        co_await txn.rollback();
        co_return;
    }

    bool ok_commit = co_await txn.commit();
    if (!ok_commit)
    {
        std::cout << "[ERROR] COMMIT failed\n";
        co_return;
    }

    std::cout << "[OK] transfer complete\n";
    co_return;
}
```

---

## Transaction config

You can control isolation level, read-only mode, and deferrable mode:

```cpp
usub::pg::PgTransactionConfig cfg{
    .isolation   = usub::pg::TxIsolationLevel::Serializable,
    .read_only   = false,
    .deferrable  = false
};

usub::pg::PgTransaction txn(&usub::pg::PgPool::instance(), cfg);
bool ok_begin = co_await txn.begin();
```

Generated `BEGIN` looks like:

* isolation level:

    * `READ COMMITTED`
    * `REPEATABLE READ`
    * `SERIALIZABLE`
* mode: `READ WRITE` or `READ ONLY`
* optionally `DEFERRABLE`

---

## Querying inside a transaction

```cpp
auto qr = co_await txn.query(
    "UPDATE users SET name = $1 WHERE id = $2 RETURNING name;",
    "John", 1
);

if (!qr.ok)
{
    std::cout << "[ERROR] update failed: " << qr.error << "\n";
    co_await txn.rollback();
    co_return;
}
```

* `txn.query(...)` calls `PgPool::query_on()` on the same pinned connection.
* You always read the result via standard `QueryResult`.

If the underlying connection is dropped mid-transaction (e.g. network failure), `txn.query()` returns with:

* `ok = false`
* `code = PgErrorCode::ConnectionClosed`
  and the transaction is automatically marked inactive.

---

## Commit / rollback

```cpp
bool ok_commit = co_await txn.commit();
if (!ok_commit)
    std::cout << "[ERROR] commit failed\n";
```

```cpp
co_await txn.rollback();
```

There is also:

```cpp
co_await txn.abort();   // uses ABORT instead of ROLLBACK
co_await txn.finish();  // "best effort cleanup": rollback if still active
```

---

## Connection handoff back to the pool

Internally, `PgTransaction` uses a dedicated connection (acquired via `PgPool::acquire_connection()`).

When the transaction ends (`commit`, `rollback`, `finish`, `abort`):

* It calls `PgPool::release_connection_async(conn)`.

`release_connection_async` is important:

* It *awaits* internal cleanup on that connection.
* It drains any remaining `PGresult`s from libpq.
* Only after the connection is known to be idle/clean, it is re-queued into the pool for reuse.

If the connection is already in a bad state (disconnected, protocol error, etc.), the pool will not recycle it — it will
retire it and decrement `live_count_`. The object will eventually be destroyed (closing the socket, calling `PQfinish`).

This design guarantees:

* A transaction will never return a “dirty” connection to the pool.
* No other coroutine will later see `"another command is already in progress"` because of leftover results from your
  transaction, COMMIT, or ROLLBACK.

---

## Subtransactions (SAVEPOINT)

`PgTransaction` supports savepoints for partial rollback:

```cpp
auto sub = txn.make_subtx();

bool ok_sub_begin = co_await sub.begin();
if (!ok_sub_begin)
{
    std::cout << "[ERROR] SAVEPOINT begin failed\n";
    // still inside main txn, but subtx didn't start
}

auto r_inner = co_await sub.query(
    "UPDATE ledger SET amount = amount + $1 WHERE id = $2 RETURNING amount;",
    500, 42
);

if (!r_inner.ok)
{
    std::cout << "[WARN] inner update failed, rolling back subtx\n";
    co_await sub.rollback();
}
else
{
    bool ok_sub_commit = co_await sub.commit();
    if (!ok_sub_commit)
        std::cout << "[WARN] subtx commit failed\n";
}
```

Semantics:

* `begin()` issues `SAVEPOINT <name>`
* `commit()` issues `RELEASE SAVEPOINT <name>`
* `rollback()` issues `ROLLBACK TO SAVEPOINT <name>`

All these run on the same underlying `PGconn`.

---

## Summary

| Feature                     | Description                                                    |
|-----------------------------|----------------------------------------------------------------|
| Dedicated connection        | Each `PgTransaction` pins one physical connection              |
| Async begin/commit/rollback | All blocking points are coroutine suspension                   |
| Safe return to pool         | Connection is drained via `release_connection_async`           |
| Automatic invalidation      | Lost connection marks the transaction as rolled back           |
| Subtransactions (SAVEPOINT) | Fine-grained rollback without aborting the parent transaction  |
| Structured results          | All queries return `QueryResult` with `ok`, `code`, `error`, … |