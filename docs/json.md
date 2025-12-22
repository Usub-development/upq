# JSON (ujson)

UPQ supports:
- writing C++ structs into `JSONB/JSON` via query parameters
- reading `JSONB/JSON` into C++ structs via `PgJson<T, Strict>`
- strict vs non-strict parsing (unknown keys, etc.)

This integration uses **ujson**.

---

## Types

### PgJson

`PgJson<T, Strict>` is a result-field wrapper decoded from JSON text.

```cpp
template<class T, bool Strict = true>
struct PgJson {
    static constexpr bool strict = Strict;
    T value{};
};
```

* `Strict=true`: unknown keys => parse error
* `Strict=false`: unknown keys are ignored (as implemented by ujson)

### PgJsonParam

`PgJsonParam<T, Strict, Jsonb>` is a parameter wrapper used to serialize an object into JSON for `INSERT/UPDATE`.

Helpers:

```cpp
template<class T, bool Strict = true>
PgJsonParam<T, Strict, true>  pg_jsonb(const T& v);

template<class T, bool Strict = true>
PgJsonParam<T, Strict, false> pg_json (const T& v);
```

Use `pg_jsonb()` in almost all cases.

---

## ujson enum mapping (enum_meta)

If your JSON contains enums, define mapping in ujson:

```cpp
enum class Role { User, Admin };

namespace ujson {
template<>
struct enum_meta<Role> {
    static inline constexpr auto items = enumerate<Role::User, Role::Admin>();
};
} // namespace ujson
```

---

## Example: write and read JSONB with PgPool

This example:

* creates a table
* inserts a valid `Profile` using `exec_reflect + pg_jsonb(Profile)`
* inserts a “broken” JSONB payload with an extra `UNKNOWN` key
* reads valid rows with strict parsing
* shows that strict parsing fails on the broken row
* reads the broken row with non-strict parsing successfully

### Models

```cpp
struct Profile {
    int age{};
    std::optional<std::string> city;
    std::vector<std::string> flags;
};

struct UserJsonRowStrict {
    int64_t id{};
    std::string username;
    usub::pg::PgJson<Profile, true> profile;   // strict
};

struct UserJsonRowLoose {
    int64_t id{};
    std::string username;
    usub::pg::PgJson<Profile, false> profile;  // non-strict
};
```

### Full coroutine demo

```cpp
usub::uvent::task::Awaitable<void> demo_pgjson_ujson(usub::pg::PgPool& pool) {
    using namespace usub::pg;

    std::cout.setf(std::ios::unitbuf);
    std::cout << "[JSON] demo start\n";

    // 1) schema
    {
        auto r = co_await pool.query_awaitable(R"SQL(
            CREATE TABLE IF NOT EXISTS users_json_demo (
                id        BIGSERIAL PRIMARY KEY,
                username  TEXT  NOT NULL,
                profile   JSONB NOT NULL
            );
        )SQL");
        if (!r.ok) { std::cerr << "[JSON/SCHEMA] " << r.error << "\n"; co_return; }

        auto t = co_await pool.query_awaitable("TRUNCATE users_json_demo RESTART IDENTITY");
        if (!t.ok) { std::cerr << "[JSON/TRUNCATE] " << t.error << "\n"; co_return; }

        std::cout << "[JSON/SCHEMA+TRUNCATE] OK\n";
    }

    // 2) insert good (C++ -> JSONB)
    {
        Profile p{ .age = 27, .city = std::string("AMS"), .flags = {"a","b"} };
        std::string name = "kirill";

        auto ins = co_await pool.exec_reflect(
            "INSERT INTO users_json_demo(username, profile) VALUES($1,$2)",
            std::tuple{name, pg_jsonb(p)}
        );
        if (!ins.ok) { std::cerr << "[JSON/INSERT good] " << ins.error << "\n"; co_return; }
        std::cout << "[JSON/INSERT good] OK\n";
    }

    // 3) insert broken (extra key)
    {
        auto ins = co_await pool.query_awaitable(R"SQL(
            INSERT INTO users_json_demo(username, profile)
            VALUES ('broken', '{"age":1,"city":"A","flags":["x"],"UNKNOWN":123}'::jsonb);
        )SQL");
        if (!ins.ok) { std::cerr << "[JSON/INSERT broken] " << ins.error << "\n"; co_return; }
        std::cout << "[JSON/INSERT broken] OK\n";
    }

    // 4) strict read: read only valid rows
    {
        auto rows = co_await pool.query_reflect_expected<UserJsonRowStrict>(
            "SELECT id, username, profile FROM users_json_demo "
            "WHERE username <> 'broken' ORDER BY id"
        );

        if (!rows) {
            std::cerr << "[JSON/SELECT strict good] FAIL code=" << toString(rows.error().code)
                      << " err='" << rows.error().error << "'\n";
        } else {
            std::cout << "[JSON/SELECT strict good] OK n=" << rows->size() << "\n";
            for (auto& r : *rows) {
                std::cout << "  id=" << r.id
                          << " username=" << r.username
                          << " age=" << r.profile.value.age
                          << " city=" << (r.profile.value.city ? *r.profile.value.city : "<NULL>")
                          << " flags=" << r.profile.value.flags.size()
                          << "\n";
            }
        }
    }

    // 5) strict read: broken row should fail
    {
        auto rows = co_await pool.query_reflect_expected<UserJsonRowStrict>(
            "SELECT id, username, profile FROM users_json_demo WHERE username='broken' LIMIT 1"
        );

        if (!rows) {
            std::cout << "[JSON/SELECT strict broken] EXPECTED FAIL code=" << toString(rows.error().code)
                      << " err='" << rows.error().error << "'\n";
        } else {
            std::cout << "[JSON/SELECT strict broken] UNEXPECTED OK\n";
        }
    }

    // 6) loose read: broken row is accepted
    {
        auto rows = co_await pool.query_reflect_expected<UserJsonRowLoose>(
            "SELECT id, username, profile FROM users_json_demo WHERE username='broken' LIMIT 1"
        );

        if (!rows) {
            std::cerr << "[JSON/SELECT loose] FAIL code=" << toString(rows.error().code)
                      << " err='" << rows.error().error << "'\n";
            co_return;
        }

        std::cout << "[JSON/SELECT loose] OK n=" << rows->size() << "\n";
        for (auto& r : *rows) {
            std::cout << "  id=" << r.id
                      << " username=" << r.username
                      << " age=" << r.profile.value.age
                      << " city=" << (r.profile.value.city ? *r.profile.value.city : "<NULL>")
                      << " flags=" << r.profile.value.flags.size()
                      << "\n";
        }
    }

    std::cout << "[JSON] demo done\n";
    co_return;
}
```

---

## Notes on debug logs

If you see logs like:

* `fields: [age] [city] [flags]`
* `key='UNKNOWN' ...`

that is your **ujson/reflect debug output** for strict parsing on a payload containing an unknown key. It’s expected while strict parsing is failing.

To silence it, disable your debug macro(s) in ujson / reflect layer (whatever emits those prints), or gate them behind a compile-time flag.