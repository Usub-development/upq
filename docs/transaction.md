# PgTransaction

`PgTransaction` provides `BEGIN` / `COMMIT` / `ROLLBACK` support on a single pooled connection.

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
        std::cout << "Error: " << res.error << "\n";
        co_await txn.rollback();
        co_await txn.finish();
        co_return;
    }

    if (!co_await txn.commit())
    {
        std::cout << "Commit failed, rolling back\n";
        co_await txn.rollback();
    }

    co_await txn.finish();
    co_return;
}
```

## API summary

| Method                | Description                                  |
|-----------------------|----------------------------------------------|
| `begin()`             | Acquires connection and sends `BEGIN`.       |
| `query(sql, args...)` | Executes query in this transaction.          |
| `commit()`            | Sends `COMMIT`.                              |
| `rollback()`          | Sends `ROLLBACK`.                            |
| `finish()`            | Returns connection to pool (must be called). |

## State accessors

```cpp
bool is_active() const noexcept;
bool is_committed() const noexcept;
bool is_rolled_back() const noexcept;
```
Use for diagnostics or logging.