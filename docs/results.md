# Result Types

upq uses structured, allocation-owned result objects for all database operations.  
No exceptions are thrown — everything is explicit and predictable.

There are three main result types:

- **`QueryResult`** — for standard SQL statements (`SELECT`, `UPDATE`, `COMMIT`, etc.)
- **`PgCopyResult`** — for bulk COPY (`COPY ... FROM STDIN`, `COPY ... TO STDOUT`)
- **`PgCursorChunk`** — for chunked cursor fetches

Each type includes consistent fields: `ok`, `code`, `error`, and `err_detail`.

---

## PgErrorCode

Every result type references `PgErrorCode` for classification:

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
````

**Meaning:**

| Code               | Meaning                                           |
|--------------------|---------------------------------------------------|
| `OK`               | Operation succeeded                               |
| `ConnectionClosed` | Socket or connection unusable                     |
| `SocketReadFailed` | Low-level I/O failure                             |
| `ServerError`      | PostgreSQL returned an error (non-00000 SQLSTATE) |
| `InvalidFuture`    | Awaited an invalid or uninitialized query         |
| `ParserTruncated*` | Row/field metadata truncated or corrupt           |
| `Unknown`          | Fallback for unspecified errors                   |

---

## QueryResult

Returned by:

* `PgPool::query_awaitable(...)`
* `PgPool::query_on(...)`
* `PgTransaction::query(...)`
* `PgConnectionLibpq::exec_simple_query_nonblocking(...)`
* `PgConnectionLibpq::exec_param_query_nonblocking(...)`
* transaction statements (`BEGIN`, `COMMIT`, `ROLLBACK`)

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

    const Row& operator[](size_t i) const noexcept { return rows[i]; }
    Row& operator[](size_t i) noexcept { return rows[i]; }

    bool ok{false};
    PgErrorCode code{PgErrorCode::Unknown};

    std::string error;
    PgErrorDetail err_detail;

    bool rows_valid{true};

    [[nodiscard]] bool empty() const noexcept { return ok && rows_valid && rows.empty(); }
    [[nodiscard]] bool has_rows() const noexcept { return ok && rows_valid && !rows.empty(); }
    [[nodiscard]] size_t row_count() const noexcept { return rows.size(); }
    [[nodiscard]] size_t col_count() const noexcept { return rows.empty() ? 0 : rows[0].cols.size(); }

    // Invariant: if rows are non-empty, each Row has non-empty cols
    [[nodiscard]] bool invariant() const noexcept { return rows.empty() || !rows[0].cols.empty(); }
};
```

### PgErrorDetail

```cpp
struct PgErrorDetail
{
    std::string sqlstate;
    std::string message;
    std::string detail;
    std::string hint;
    PgSqlStateClass category; // UniqueViolation, Deadlock, etc.
};
```

### Semantics

| Field        | Meaning                                  |
|--------------|------------------------------------------|
| `ok`         | High-level success indicator             |
| `code`       | Low-level classification (`PgErrorCode`) |
| `error`      | Human-readable message                   |
| `rows`       | Result rows (may be empty)               |
| `rows_valid` | `false` means truncated or unsafe result |
| `err_detail` | Parsed SQLSTATE + diagnostic info        |

**Row invariants:**

* All rows have identical column counts.
* If `rows` is not empty, each `Row::cols` is also non-empty.
* If `ok == true` and `rows.empty() == true`, the query succeeded but returned no data.

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
    std::cout << "[INFO] no rows found\n";
}
else
{
    for (auto& row : res.rows)
        std::cout << row[0] << " " << row[1] << "\n";
}
```

---

## Additional Helpers

### QueryResult methods

| Method                              | Description                                           | Returns          |
|-------------------------------------|-------------------------------------------------------|------------------|
| `bool empty() const noexcept`       | `true` if query succeeded but returned **zero rows**. | `true` / `false` |
| `bool has_rows() const noexcept`    | `true` if query succeeded and returned ≥1 row.        | `true` / `false` |
| `size_t row_count() const noexcept` | Number of rows in the result.                         | Count            |
| `size_t col_count() const noexcept` | Number of columns per row (0 if empty).               | Count            |
| `bool invariant() const noexcept`   | Ensures: non-empty `rows` ⇒ non-empty `cols`.         | `true` / `false` |

**Example:**

```cpp
auto res = co_await pool.query_awaitable("SELECT id, name FROM users");
if (res.empty())
    std::cout << "[INFO] no rows\n";
else
    std::cout << "rows=" << res.row_count()
              << " cols=" << res.col_count() << "\n";
```

---

### Row methods

| Method                         | Description                                                         | Returns                               |
|--------------------------------|---------------------------------------------------------------------|---------------------------------------|
| `size_t size() const noexcept` | Number of columns in the row.                                       | Column count                          |
| `bool empty() const noexcept`  | `true` if row has zero columns (shouldn’t happen in valid results). | `true` / `false`                      |
| `operator[](size_t i)`         | Access column by index.                                             | `std::string&` / `const std::string&` |

**Example:**

```cpp
for (auto& row : res.rows)
{
    std::cout << "row size=" << row.size() << "\n";
    std::cout << row[0] << " " << row[1] << "\n";
}
```

---

## PgCopyResult

Returned by all COPY operations.

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

**Notes:**

* For `COPY ... FROM STDIN`, `rows_affected` is valid after `copy_in_finish()`.
* For `COPY ... TO STDOUT`, `copy_out_start()` returns an initial result; actual data arrives in chunks.

---

## PgCursorChunk

Used for incremental cursor fetching:

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

`done = true` indicates end-of-cursor.

---

## Summary

| Type            | Purpose                 | Primary data    | End-of-stream signal                       |
|-----------------|-------------------------|-----------------|--------------------------------------------|
| `QueryResult`   | Normal SQL / Tx control | `rows`          | N/A                                        |
| `PgCopyResult`  | COPY IN/OUT             | `rows_affected` | `copy_in_finish()` or empty COPY OUT chunk |
| `PgCursorChunk` | Cursor fetch            | `rows`          | `done == true` or empty `rows`             |

---

### Common Patterns

* Always check `ok` before using data.
* Use `empty()` to detect successful queries with no rows.
* Use `has_rows()` for success with results.
* `rows_valid == false` → truncated/unsafe result.
* No exceptions; all information is explicit and type-safe.