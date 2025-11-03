# Result Types

`upq` returns structured, ownership-safe result objects for every operation.  
No exceptions — all outcomes are explicit and typed.

There are three main result types:

- **`QueryResult`** — standard SQL results (`SELECT`, `UPDATE`, `COMMIT`, etc.)
- **`PgCopyResult`** — bulk I/O results (`COPY ... FROM/TO`)
- **`PgCursorChunk`** — streamed fetch chunks for server-side cursors

Each contains `ok`, `code`, `error`, and `err_detail` fields for consistent diagnostics.

---

## Reflect Integration

With reflection-based APIs (`query_reflect`, `query_reflect_one`, `exec_reflect`),  
you typically **don’t access `QueryResult` directly** — the library maps results into aggregates automatically.

| Operation                | Return type        | Description                                             |
|--------------------------|--------------------|---------------------------------------------------------|
| `query_reflect<T>()`     | `std::vector<T>`   | SELECT → struct or tuple list (name- or position-based) |
| `query_reflect_one<T>()` | `std::optional<T>` | SELECT one row → optional                               |
| `exec_reflect()`         | `QueryResult`      | Executes using struct/tuple as parameters               |

Example:

```cpp
struct User {
    int64_t id;
    std::string username; // maps from "name AS username"
    std::optional<std::string> password;
};

auto users = co_await pool.query_reflect<User>(
    "SELECT id, name AS username, password FROM users;"
);

for (auto& u : users)
    std::cout << "id=" << u.id << " name=" << u.username << "\n";
```

Internally, `upq` still constructs a `QueryResult` — reflection just translates between `rows` and your C++ types.
To debug raw SQL behavior, use `query_awaitable()`.

---

## PgErrorCode

Every result embeds a `PgErrorCode` for classification:

```cpp
enum class PgErrorCode : uint32_t {
    OK = 0,
    InvalidFuture,
    ConnectionClosed,
    SocketReadFailed,
    ProtocolCorrupt,
    ParserTruncatedField,
    ParserTruncatedRow,
    ParserTruncatedHeader,
    ServerError,
    AuthFailed,
    AwaitCanceled,
    Unknown
};
```

| Code               | Meaning                                           |
|--------------------|---------------------------------------------------|
| `OK`               | Operation succeeded                               |
| `ConnectionClosed` | Socket/PGconn unusable                            |
| `SocketReadFailed` | I/O error during read or flush                    |
| `ServerError`      | PostgreSQL returned an error (non-00000 SQLSTATE) |
| `InvalidFuture`    | Query awaited after invalidation                  |
| `ParserTruncated*` | Corrupted or incomplete row/field metadata        |
| `Unknown`          | Fallback category                                 |

---

## QueryResult

Returned by:

* `PgPool::query_awaitable(...)`
* `PgPool::query_on(...)`
* `PgTransaction::query(...)`
* `PgConnectionLibpq::exec_*_nonblocking(...)`
* Transaction control commands (`BEGIN`, `COMMIT`, `ROLLBACK`)

### Structure

```cpp
struct QueryResult
{
    struct Row
    {
        std::vector<std::string> cols;

        const std::string& operator[](size_t i) const noexcept { return cols[i]; }
        std::string& operator[](size_t i) noexcept { return cols[i]; }

        [[nodiscard]] size_t size() const noexcept { return cols.size(); }
        [[nodiscard]] bool empty() const noexcept { return cols.empty(); }
    };

    std::vector<Row> rows;

    bool ok{false};
    PgErrorCode code{PgErrorCode::Unknown};

    std::string error;
    PgErrorDetail err_detail;

    bool rows_valid{true};

    [[nodiscard]] bool empty() const noexcept { return ok && rows_valid && rows.empty(); }
    [[nodiscard]] bool has_rows() const noexcept { return ok && rows_valid && !rows.empty(); }
    [[nodiscard]] size_t row_count() const noexcept { return rows.size(); }
    [[nodiscard]] size_t col_count() const noexcept { return rows.empty() ? 0 : rows[0].cols.size(); }
};
```

---

### PgErrorDetail

```cpp
struct PgErrorDetail
{
    std::string sqlstate;
    std::string message;
    std::string detail;
    std::string hint;
    PgSqlStateClass category; // UniqueViolation, DeadlockDetected, etc.
};
```

---

### Semantics

| Field        | Meaning                                    |
|--------------|--------------------------------------------|
| `ok`         | Operation success flag                     |
| `code`       | `PgErrorCode` classification               |
| `error`      | Human-readable error message               |
| `rows`       | Result rows (may be empty)                 |
| `rows_valid` | False → truncated or unsafe data           |
| `err_detail` | Server diagnostics (SQLSTATE, hints, etc.) |

Row invariants:

* All rows have identical column counts.
* Non-empty rows always have non-empty `cols`.
* `ok && rows.empty()` → query succeeded, returned zero rows.

---

### Example

```cpp
auto res = co_await pool.query_awaitable(
    "SELECT id, name FROM users WHERE id = $1;", 1
);

if (!res.ok)
{
    std::cout << "[ERROR] " << res.error
              << " sqlstate=" << res.err_detail.sqlstate
              << " category=" << (int)res.err_detail.category
              << "\n";
}
else if (res.empty())
{
    std::cout << "[INFO] no rows\n";
}
else
{
    for (auto& row : res.rows)
        std::cout << row[0] << " " << row[1] << "\n";
}
```

---

## PgCopyResult

Used for all COPY operations.

```cpp
struct PgCopyResult
{
    bool ok{false};
    PgErrorCode code{PgErrorCode::Unknown};

    std::string error;
    PgErrorDetail err_detail;

    uint64_t rows_affected{0};
};
```

* For `COPY FROM STDIN`, `rows_affected` is valid after `copy_in_finish()`.
* For `COPY TO STDOUT`, `copy_out_start()` yields this, and data follows via chunks.

---

## PgCursorChunk

Used for incremental, streamed fetches:

```cpp
struct PgCursorChunk
{
    std::vector<QueryResult::Row> rows;

    bool done{false};
    bool ok{false};
    PgErrorCode code{PgErrorCode::Unknown};

    std::string error;
    PgErrorDetail err_detail;
};
```

`done == true` indicates end-of-stream or exhausted cursor.

---

## Summary

| Type            | Purpose                 | Data field      | End signal                     |
|-----------------|-------------------------|-----------------|--------------------------------|
| `QueryResult`   | Normal SQL / Tx control | `rows`          | N/A                            |
| `PgCopyResult`  | COPY IN/OUT             | `rows_affected` | `copy_in_finish()` / EOF chunk |
| `PgCursorChunk` | Cursor streaming        | `rows`          | `done == true` or empty `rows` |

---

### Common Usage Notes

* Always check `ok` before using data.
* `empty()` → successful query with zero rows.
* `rows_valid == false` → truncated or partial stream.
* No exceptions — all outcomes are explicit, structured, and coroutine-friendly.