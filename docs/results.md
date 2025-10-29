# Result Types

upq uses structured, allocation-owned result objects for all database-facing operations.  
No exceptions are thrown. Everything is explicit.

There are three primary result types:

- `QueryResult` — standard SQL statements (`SELECT`, `UPDATE`, `COMMIT`, etc.)
- `PgCopyResult` — bulk COPY (`COPY ... FROM STDIN`, `COPY ... TO STDOUT` end result)
- `PgCursorChunk` — chunked fetch from a server-side cursor

All of them expose consistent `ok`, `code`, and `error` fields.

---

## PgErrorCode

All result types reference `PgErrorCode`:

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

Meaning:

* `OK`: operation succeeded
* `ConnectionClosed`: the connection/socket was unusable
* `SocketReadFailed`: low-level I/O failure while reading/writing
* `ServerError`: PostgreSQL returned an error (non-00000 SQLSTATE)
* `InvalidFuture`: you called something in an invalid state (e.g. query outside active transaction)
* `ParserTruncated*`: row/field metadata was incomplete or corrupted
* `Unknown`: fallback

---

## QueryResult

Returned by:

* `PgPool::query_awaitable(...)`
* `PgPool::query_on(...)`
* `PgTransaction::query(...)`
* `PgConnectionLibpq::exec_simple_query_nonblocking(...)`
* `PgConnectionLibpq::exec_param_query_nonblocking(...)`
* transaction control (`BEGIN`, `COMMIT`, `ROLLBACK`, etc.)

```cpp
struct QueryResult
{
    struct Row
    {
        std::vector<std::string> cols;
    };

    std::vector<Row> rows;

    bool ok{false};
    PgErrorCode code{PgErrorCode::Unknown};

    std::string error;

    PgErrorDetail err_detail; // sqlstate, detail, hint, category

    bool rows_valid{true};
};
```

`err_detail`:

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

### Fields

* `ok`: high-level success
* `code`: machine-readable classification (`PgErrorCode`)
* `error`: human-readable message
* `rows`: result rows (if any)
* `rows_valid`: `false` means partial/unsafe result (protocol was cut mid-stream)
* `err_detail`: parsed server diagnostics, including `sqlstate` and category

### Example

```cpp
auto res = co_await pool.query_awaitable(
    "SELECT id, name FROM users WHERE id = $1;", 1
);

if (!res.ok)
{
    std::cout
        << "[ERROR] " << res.error
        << " sqlstate=" << res.err_detail.sqlstate
        << " category=" << (int)res.err_detail.category
        << "\n";
}
else
{
    for (auto& row : res.rows)
        std::cout << row.cols[0] << " " << row.cols[1] << "\n";
}
```

---

## PgCopyResult

Returned by:

* `PgConnectionLibpq::copy_in_start(...)`
* `PgConnectionLibpq::copy_in_send_chunk(...)`
* `PgConnectionLibpq::copy_in_finish(...)`
* `PgConnectionLibpq::copy_out_start(...)`
* end-of-copy status (after COPY OUT completes)

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

Notes:

* For `COPY ... FROM STDIN`, `rows_affected` is populated after `copy_in_finish()`.
* `copy_in_send_chunk()` returns `ok=true` if the chunk was accepted and flushed.
* For `COPY ... TO STDOUT`, `copy_out_start(...)` returns a `PgCopyResult` telling you whether COPY OUT mode entered
  successfully. The actual row data then comes from `copy_out_read_chunk()`.

### Example: COPY IN

```cpp
auto st = co_await conn->copy_in_start(
    "COPY public.bigdata(payload) FROM STDIN"
);
if (!st.ok) { /* handle error */ }

for (int i = 0; i < 5; i++)
{
    std::string line = "payload line " + std::to_string(i) + "\n";
    auto put = co_await conn->copy_in_send_chunk(
        line.data(), line.size()
    );
    if (!put.ok) { /* handle error */ }
}

auto fin = co_await conn->copy_in_finish();
if (!fin.ok)
{
    std::cout << "[ERROR] COPY IN finish: " << fin.error << "\n";
}
else
{
    std::cout << "[INFO] COPY IN done, rows=" << fin.rows_affected << "\n";
}
```

### COPY OUT streaming

Data chunks are retrieved separately:

```cpp
auto start = co_await conn->copy_out_start(
    "COPY (SELECT id, payload FROM public.bigdata ORDER BY id) TO STDOUT"
);

if (!start.ok)
{
    std::cout << "[ERROR] COPY OUT start: " << start.error << "\n";
    co_return;
}

for (;;)
{
    auto chunk = co_await conn->copy_out_read_chunk();
    if (!chunk.ok)
    {
        std::cout << "[ERROR] COPY OUT read: " << chunk.err.message << "\n";
        break;
    }

    if (chunk.value.empty())
    {
        std::cout << "[INFO] COPY OUT finished\n";
        break;
    }

    std::string s(chunk.value.begin(), chunk.value.end());
    std::cout << "[COPY-OUT-CHUNK] " << s;
}
```

`copy_out_read_chunk()` returns `PgWireResult<std::vector<uint8_t>>`:

* `ok=true` and non-empty `value` → streamed chunk of COPY data
* `ok=true` and empty `value` → COPY is finished (end-of-stream)
* `ok=false` → error (with `err.code` and `err.message`)

---

## PgCursorChunk

Returned by:

* `PgConnectionLibpq::cursor_fetch_chunk(...)`

Also indirectly influenced by:

* `cursor_declare(...)`
* `cursor_close(...)`

This is for server-side cursors and incremental fetch.

```cpp
struct PgCursorChunk
{
    std::vector<QueryResult::Row> rows;

    bool done{false};           // true when cursor is exhausted
    bool ok{false};
    PgErrorCode code{PgErrorCode::Unknown};

    std::string error;
    PgErrorDetail err_detail;
};
```

### Typical flow

```cpp
auto decl = co_await conn->cursor_declare(
    cursor_name,
    "SELECT id, payload FROM public.bigdata ORDER BY id"
);
if (!decl.ok) {
    std::cout << "[ERROR] DECLARE CURSOR failed: " << decl.error << "\n";
    co_return;
}

for (;;)
{
    PgCursorChunk ck =
        co_await conn->cursor_fetch_chunk(cursor_name, 3);

    if (!ck.ok)
    {
        std::cout << "[ERROR] FETCH failed: " << ck.error << "\n";
        break;
    }

    if (ck.rows.empty())
    {
        std::cout << "[INFO] cursor done\n";
        break;
    }

    for (auto& row : ck.rows)
    {
        std::cout << "id=" << row.cols[0]
                  << " payload=" << row.cols[1] << "\n";
    }

    if (ck.done)
        break;
}

auto cls = co_await conn->cursor_close(cursor_name);
if (!cls.ok)
    std::cout << "[WARN] cursor close failed: " << cls.error << "\n";
```

### Notes

* `done=true` means no more rows (end of cursor / COMMIT happened).
* Even if a chunk returns `ok=true`, `rows` may be empty if the cursor has been fully consumed.
* `cursor_close()` finalizes the `CLOSE <cursor>; COMMIT;` sequence and returns a `QueryResult`.

---

## Summary

| Type            | Used for                                       | Data field      | End-of-stream signal                                                |
|-----------------|------------------------------------------------|-----------------|---------------------------------------------------------------------|
| `QueryResult`   | Normal SQL / Tx control                        | `rows`          | N/A                                                                 |
| `PgCopyResult`  | COPY IN begin/chunk/end,<br>COPY OUT start/end | `rows_affected` | End is signaled by `copy_in_finish()` or empty chunk after COPY OUT |
| `PgCursorChunk` | FETCH FORWARD from server cursor               | `rows`          | `done == true` or empty `rows`                                      |

Everything follows the same pattern:

* `ok` and `code` tell you if the step worked.
* `error` + `err_detail` tell you why it didn't.
* No exceptions are thrown.