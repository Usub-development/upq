# PgTransaction

`PgTransaction` provides coroutine-safe transaction management on top of `PgPool`.  
It wraps a single connection and supports `BEGIN`, `COMMIT`, `ROLLBACK`, and `ABORT` with full error transparency.

---

## Overview

Each transaction owns one dedicated connection from the pool.  
All operations (`BEGIN`, `COMMIT`, `ROLLBACK`, `ABORT`) are asynchronous and return structured `QueryResult` or `bool`.

After the transaction ends (committed, rolled back, or aborted), the connection is automatically released back to the
pool.

---

## Example

```cpp
#include "upq/PgTransaction.h"

using namespace usub;

uvent::task::Awaitable<void> transfer_example()
{
    pg::PgTransaction txn;

    if (!co_await txn.begin())
    {
        std::cout << "Transaction begin failed\n";
        co_await txn.finish();
        co_return;
    }

    auto res = co_await txn.query(
        "UPDATE users SET balance = balance - $1 WHERE id = $2 RETURNING balance;",
        100.0, 42
    );

    if (!res.ok)
    {
        std::cout << "Query failed: " << res.error << "\n";
        co_await txn.abort(); // soft abort instead of rollback
        co_await txn.finish();
        co_return;
    }

    if (!co_await txn.commit())
    {
        std::cout << "Commit failed — aborting\n";
        co_await txn.abort();
    }

    co_await txn.finish();
    co_return;
}
```

---

## API summary

| Method                | Description                                       |
|-----------------------|---------------------------------------------------|
| `begin()`             | Starts a new transaction (`BEGIN`).               |
| `query(sql, args...)` | Executes a query within the active transaction.   |
| `commit()`            | Commits the transaction (`COMMIT`).               |
| `rollback()`          | Performs an explicit rollback (`ROLLBACK`).       |
| `abort()`             | Performs a soft abort (`ABORT` if connected).     |
| `finish()`            | Ensures cleanup; calls `abort()` if still active. |

---

## `abort()`

`abort()` safely terminates a transaction using a lightweight approach.

* If the connection is alive, sends the PostgreSQL command `ABORT`
  (an alias of `ROLLBACK`).
* If the connection is already broken, marks the transaction as rolled back
  and releases the connection locally.

Used when:

* Coroutine cancelled mid-transaction
* Connection lost (`ConnectionClosed`)
* You want to discard transaction without waiting for a full rollback

```cpp
if (res.code == PgErrorCode::ConnectionClosed)
{
    std::cout << "Connection lost — aborting transaction\n";
    co_await txn.abort();
    co_await txn.finish();
    co_return;
}
```

---

## Error propagation

Transactions propagate structured errors with `PgErrorCode`:

| Condition            | `PgErrorCode`      | Notes                          |
|----------------------|--------------------|--------------------------------|
| Inactive transaction | `InvalidFuture`    | No `BEGIN` or already finished |
| Connection dropped   | `ConnectionClosed` | Socket or PGconn lost mid-txn  |
| Server-side error    | `ServerError`      | SQLSTATE and details available |
| Commit failure       | `ServerError`      | COMMIT rejected or timed out   |
| Abort failure        | `SocketReadFailed` | Connection failed during abort |

Example:

```cpp
auto res = co_await txn.query("UPDATE accounts SET ...;");
if (!res.ok) {
    std::cout
        << "[TXN ERROR] code=" << (uint32_t)res.code
        << " msg=" << res.error
        << " sqlstate=" << res.server_sqlstate << std::endl;
}
```

---

## `finish()`

`finish()` finalizes the transaction and always returns the connection to the pool.
If the transaction is still active, it automatically calls `abort()`.

This means you can safely call `finish()` in every exit path:

```cpp
pg::PgTransaction txn;
if (!co_await txn.begin()) co_return;

// ...

co_await txn.finish(); // always safe
```

---

## Lifecycle

| Stage    | Method       | Description                                    |
|----------|--------------|------------------------------------------------|
| Start    | `begin()`    | Sends `BEGIN;` and marks active                |
| Execute  | `query()`    | Runs statements on same connection             |
| Commit   | `commit()`   | Sends `COMMIT;`                                |
| Abort    | `abort()`    | Sends `ABORT;` or marks rolled back            |
| Rollback | `rollback()` | Sends `ROLLBACK;` explicitly                   |
| Finish   | `finish()`   | Releases connection (uses `abort()` if active) |

---

## Design intent

`abort()` fills the gap between graceful rollback and hard disconnect recovery.
It lets coroutines cancel transactions fast and deterministically without waiting for PostgreSQL response in broken or
cancelled states.

`finish()` now consistently defers to `abort()` for safe cleanup,
making the transaction API idempotent and fault-tolerant.