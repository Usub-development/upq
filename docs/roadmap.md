# Roadmap

The following roadmap outlines planned extensions and improvements for **upq**.  
Each milestone builds upon the current asynchronous architecture — focusing on safety, throughput, and observability.

---

## 1. PgReconnectSupervisor — Automatic Reconnection Layer

A coroutine-based supervisor that monitors the connection pool’s state and restores missing connections.

**Key goals:**

- Keep `live_count_` at maximum even after disconnections or restarts.
- Recreate dropped connections asynchronously using `connect_async()`.
- Prevent cold-start latency after transient network or PostgreSQL failures.

**Status:** Planned  
**Priority:** High (stability phase)

---

## 2. PgStatsRegistry — Real-Time Metrics

Centralized registry for internal statistics:

| Metric                | Description                       |
|-----------------------|-----------------------------------|
| `total_queries`       | Number of executed SQL commands   |
| `errors`              | Query failures (client or server) |
| `bytes_in/out`        | Transferred payload size          |
| `avg_latency_us`      | Mean query latency                |
| `commits / rollbacks` | Transaction lifecycle counts      |

Metrics are automatically updated within `PgConnectionLibpq` and can be exported in Prometheus-compatible format
via coroutine endpoint (`metrics_dump()` or `/metrics` route).

**Goal:** native observability without third-party exporters.  
**Status:** Design stage.

---

## 3. PreparedStatementRegistry — Shared Statement Cache

Global SQL→statement registry reused across connections.

**Features:**

- Shared mapping of prepared statement names.
- Auto-synchronization on connection creation.
- Transparent reuse of `PREPARE`/`EXECUTE`.

**Goal:** reduce parsing overhead for repetitive queries.  
**Status:** Scheduled after metrics module.

---

## 4. PgChannelDispatcher — Multiplexed NOTIFY Router

Extension of `PgNotificationMultiplexer` supporting advanced event routing.

**Planned features:**

- Multiple handlers per channel.
- Wildcard channels (`events.*`).
- Optional per-thread dispatch using uvent message queues.

**Goal:** scalable reactive event system.  
**Status:** Concept ready.

---

## 5. PgCronScheduler — SQL-Based Job Runner

Coroutine-based scheduler driven by a database table:

```sql
CREATE TABLE cron_jobs
(
    id          serial primary key,
    name        text,
    interval_ms bigint,
    last_run    timestamptz,
    query       text
);
````

Each job is periodically executed by a background coroutine.

**Goal:** lightweight in-database automation.
**Status:** Design draft.

---

## 6. PgTypeRegistry — Binary Type Converters

Registry mapping PostgreSQL OIDs to native converters:

| OID       | Native type   |
|-----------|---------------|
| `INT8`    | `int64_t`     |
| `NUMERIC` | `BigDecimal`  |
| `JSONB`   | `std::string` |

This enables binary-protocol parsing and reduces per-row overhead.

**Goal:** high-speed deserialization and reflection support.
**Status:** Exploratory.

---

## 7. PgMigrator — Migration Manager

Migration system using SQL files (`migrations/*.sql`) with transactional apply/rollback.

**Features:**

* `migrations_log` table to track applied versions.
* Supports `--up` / `--down`.
* Fully coroutine-compatible.

**Goal:** schema management built into runtime.
**Status:** Planned after TypeRegistry & ReflectSystem.

---

## 8. PgTransactionScope — Scoped Transaction Context

RAII-style coroutine helper simplifying transactional flow:

```cpp
co_await pg::PgTransactionScope([](pg::PgTransaction& txn) -> task::Awaitable<void> {
    co_await txn.query("UPDATE accounts SET balance = balance - 10;");
    co_await txn.commit();
});
```

Automatically calls `finish()` on exit — even under exceptions.

**Goal:** cleaner coroutine transaction control.
**Status:** Ready for prototype.

---

## 9. PgIntrospector — Schema and Metadata Reflection

Asynchronous API for schema inspection:

* Table and column discovery via `information_schema`.
* Index and trigger listing.
* Type mapping extraction.

Foundation for future ORM-style or codegen tooling.

**Goal:** runtime schema reflection and introspection.
**Status:** Mid-term.

---

## 11. PgTestHarness — Transactional Testing Utility

Test helper for SQL regression and snapshot testing.

**Features:**

* Isolated temporary schema per test.
* Automatic rollback after execution.
* Snapshot comparison of `QueryResult`.

**Goal:** reproducible database testing environment.
**Status:** Deferred (post-stabilization).

---

## Summary Timeline

| Phase | Modules                                      | Focus                            |
|-------|----------------------------------------------|----------------------------------|
| **1** | ReconnectSupervisor, StatsRegistry           | Stability & observability        |
| **2** | PreparedStatementRegistry, ChannelDispatcher | Throughput & reactive events     |
| **3** | CronScheduler, TypeRegistry, ReflectSystem   | Automation & binary performance  |
| **4** | Migrator, TransactionScope                   | Schema control & DX improvements |
| **5** | Introspector, TestHarness                    | Ecosystem & developer tooling    |

---

## Long-Term Vision

**upq** will evolve toward a full coroutine-native PostgreSQL runtime featuring:

* fully async binary protocol support,
* reflection-based data mapping,
* persistent prepared statement cache,
* integrated metrics and reconnection layers,
* migration + cron orchestration,
* zero-external dependency design.