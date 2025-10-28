# QueryResult

`QueryResult` is the unified return type of all asynchronous PostgreSQL operations in **upq**.  
It encapsulates both result data and detailed diagnostic information.

All database interfaces — `PgPool`, `PgTransaction`, and `PgConnectionLibpq` — return this structure to ensure
zero-exception, fully traceable error propagation.

---

## Overview

Every PostgreSQL command — whether `SELECT`, `UPDATE`, or `LISTEN` — produces a `QueryResult`.  
It represents **either** a successful result set, **or** a structured failure with full diagnostic context.

Typical usage:

```cpp
auto res = co_await pool.query_awaitable(
    "SELECT id, name FROM users WHERE id = $1;",
    1
);

if (!res.ok)
{
    std::cout << "Query failed: " << res.error
              << " (code=" << (uint32_t)res.code
              << ", sqlstate=" << res.server_sqlstate << ")\n";
}
else
{
    for (auto& row : res.rows)
        std::cout << row.cols[0] << " | " << row.cols[1] << "\n";
}
```

---

## Structure

```cpp
enum class PgErrorCode : uint32_t {
    OK = 0,
    InvalidFuture,          // Misused transaction or invalid coroutine state
    ConnectionClosed,       // PGconn invalid or connection lost
    SocketReadFailed,       // PQflush / PQconsumeInput / socket I/O failure
    ProtocolCorrupt,        // Unexpected frame / protocol desync
    ParserTruncatedField,   // Incomplete field payload
    ParserTruncatedRow,     // Partial row read
    ParserTruncatedHeader,  // Missing column metadata
    ServerError,            // PostgreSQL returned SQL error
    AuthFailed,             // Authentication rejected (reserved)
    AwaitCanceled,          // Awaitable canceled externally
    Unknown                 // Generic / fallback
};

struct QueryResult {
    struct Row { std::vector<std::string> cols; };

    std::vector<Row> rows;

    bool ok{false};
    PgErrorCode code{PgErrorCode::Unknown};
    std::string error;

    std::string server_sqlstate;
    std::string server_detail;
    std::string server_hint;

    bool rows_valid{true};
};
```

---

## Field reference

| Field             | Type               | Description                                                                                                                  |
|-------------------|--------------------|------------------------------------------------------------------------------------------------------------------------------|
| `ok`              | `bool`             | `true` if the query succeeded and server acknowledged completion.                                                            |
| `code`            | `PgErrorCode`      | Machine-readable error category (see below).                                                                                 |
| `error`           | `std::string`      | Human-readable description (either client-side or PostgreSQL message).                                                       |
| `server_sqlstate` | `std::string`      | SQLSTATE from PostgreSQL diagnostics (`PG_DIAG_SQLSTATE`).                                                                   |
| `server_detail`   | `std::string`      | Extended detail from PostgreSQL (`PG_DIAG_MESSAGE_DETAIL`).                                                                  |
| `server_hint`     | `std::string`      | Server hint / suggested fix (`PG_DIAG_MESSAGE_HINT`).                                                                        |
| `rows`            | `std::vector<Row>` | Fetched data (if any). Each `Row` stores `cols` as strings.                                                                  |
| `rows_valid`      | `bool`             | Indicates whether the data is complete and safe to read. False if the stream was truncated or connection dropped mid-result. |

---

## PgErrorCode reference

| Code               | Meaning                                                         |
|--------------------|-----------------------------------------------------------------|
| `OK`               | Successful operation.                                           |
| `ConnectionClosed` | Connection invalid or socket closed unexpectedly.               |
| `SocketReadFailed` | Network or I/O error during PQsend/PQflush/PQconsumeInput.      |
| `ServerError`      | PostgreSQL returned SQLSTATE != `00000`.                        |
| `InvalidFuture`    | Operation invoked on inactive transaction or invalid coroutine. |
| `ProtocolCorrupt`  | Protocol desynchronization (rare, low-level).                   |
| `ParserTruncated*` | Incomplete field or tuple received.                             |
| `AwaitCanceled`    | Operation aborted before completion.                            |
| `Unknown`          | Fallback when error source is undetermined.                     |

---

## Behavioral contract

1. **Never throws exceptions** — all results are contained in `QueryResult`.
2. **Always initialized** — `ok`, `code`, and `error` are safe to read.
3. **PostgreSQL errors** include SQLSTATE, detail, and hint automatically.
4. **Incomplete data** is detectable via `rows_valid == false`.
5. **Connection failures** propagate as `ConnectionClosed` with no data.

---

## Examples

### 1. Successful SELECT

```cpp
auto res = co_await pool.query_awaitable("SELECT 1;");
if (res.ok)
    std::cout << "Result: " << res.rows[0].cols[0] << "\n";
```

**Output:**

```
Result: 1
```

---

### 2. Server error

```cpp
auto res = co_await pool.query_awaitable("SELECT * FROM nonexistent;");
if (!res.ok)
{
    std::cout
        << "Server error: " << res.error << "\n"
        << "SQLSTATE: " << res.server_sqlstate << "\n"
        << "Detail: " << res.server_detail << "\n"
        << "Hint: " << res.server_hint << "\n";
}
```

**Possible output:**

```
Server error: relation "nonexistent" does not exist
SQLSTATE: 42P01
Detail:
Hint: Check table name or schema
```

---

### 3. Connection dropped

```cpp
auto res = co_await pool.query_awaitable("SELECT now();");
if (res.code == PgErrorCode::ConnectionClosed)
    std::cout << "Connection lost — attempting to reconnect\n";
```

---

### 4. Transaction misuse

```cpp
pg::PgTransaction txn;
auto res = co_await txn.query("SELECT 1;");
if (res.code == PgErrorCode::InvalidFuture)
    std::cout << "Attempted query outside active transaction\n";
```

---

## Usage patterns

### Defensive querying

```cpp
auto res = co_await pool.query_awaitable(sql);
if (!res.ok)
{
    if (res.code == PgErrorCode::ServerError)
        log_sql_error(res.server_sqlstate, res.error);
    else
        log_client_error(res.code, res.error);
    co_return;
}
```

### Safe data iteration

```cpp
if (res.ok && res.rows_valid)
    for (auto& row : res.rows)
        process_row(row);
else
    std::cout << "Discarding partial result set\n";
```

---

## Design philosophy

Traditional PostgreSQL clients often mix error text with data flow.
`QueryResult` enforces **structured, side-effect-free error transport**, aligning with coroutine semantics:

* no exceptions,
* no blocking,
* complete error introspection at call site.

This design ensures that even under partial I/O failure, the caller always receives deterministic and debuggable
feedback.

---