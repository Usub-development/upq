# QueryResult

Returned by all query functions:

- `PgPool::query_awaitable`
- `PgPool::query_on`
- `PgTransaction::query`

---

## Structure

```cpp
struct QueryResult
{
    struct Row
    {
        std::vector<std::string> cols;
    };

    std::vector<Row> rows;
    bool ok{false};
    std::string error;
};
```

## Fields

| Field   | Meaning                                 |
|---------|-----------------------------------------|
| `ok`    | `true` if query completed successfully. |
| `error` | Error message (if `ok == false`).       |
| `rows`  | All result rows (for SELECT/RETURNING). |

## Examples
```cpp
auto res = co_await usub::pg::PgPool::instance().query_awaitable(
    "SELECT id, name FROM users WHERE id = $1;",
    1
);

if (!res.ok)
    std::cout << "DB error: " << res.error << "\n";
else if (res.rows.empty())
    std::cout << "No rows\n";
else
    for (auto& row : res.rows)
        std::cout << "id=" << row.cols[0] << " name=" << row.cols[1] << "\n";
```

## Commands (INSERT / UPDATE / DELETE)

```cpp
auto res = co_await pool.query_awaitable(
    "UPDATE users SET password = $1 WHERE id = $2;",
    "hashed_pw", 1
);

if (res.ok)
    std::cout << "Password updated\n";
else
    std::cout << "Update failed: " << res.error << "\n";
```

If the command doesn’t return tuples:
- ok == true means success
- rows empty
- error empty (unless PostgreSQL returned one)