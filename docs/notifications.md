# Notifications (LISTEN / NOTIFY)

`PgNotificationListener` provides an asynchronous interface to PostgreSQL’s built-in pub/sub system via `LISTEN` and
`NOTIFY`.  
It allows your application to react to database events in real time — without polling or triggers.

---

## Overview

`PgNotificationListener` integrates directly with `uvent`’s coroutine runtime and `PgConnectionLibpq`.  
It runs a dedicated event loop that blocks (asynchronously) on the socket and dispatches notifications as independent
coroutines.

Typical uses:

- reactive cache invalidation
- ledger and balance updates
- cross-process coordination (`NOTIFY reload_config`)
- reactive microservice event signaling

---

## Architecture

Each listener owns **one dedicated PostgreSQL connection** obtained from `PgPool`.  
That connection:

1. Executes `LISTEN <channel>;`
2. Waits asynchronously for socket readability (`poll/epoll` via `uvent`)
3. Calls `PQconsumeInput()` to pull notifications
4. Extracts messages via `PQnotifies()`
5. Dispatches handler coroutines concurrently

If the connection breaks or PostgreSQL reports `CONNECTION_BAD`, the loop exits immediately.

---

## Handler concept

Handlers are coroutine callables that satisfy the `PgNotifyHandler` concept:

```cpp
usub::uvent::task::Awaitable<void>
operator()(std::string channel,
           std::string payload,
           int backend_pid);
```

* `channel` — the name of the channel that emitted the notification
* `payload` — the string payload (may be empty)
* `backend_pid` — the PostgreSQL backend process ID that sent it

Each notification spawns its own coroutine, so handlers run concurrently and independently.

---

## Example: basic listener

```cpp
struct MyNotifyHandler
{
    usub::uvent::task::Awaitable<void>
    operator()(std::string channel,
               std::string payload,
               int backend_pid)
    {
        std::cout
            << "[NOTIFY] channel=" << channel
            << " pid=" << backend_pid
            << " payload=" << payload << std::endl;

        co_return;
    }
};
```

Setup and run:

```cpp
auto& pool = usub::pg::PgPool::instance();
auto conn = co_await pool.acquire_connection();

if (!conn || !conn->connected()) {
    std::cout << "Cannot initialize listener\n";
    co_return;
}

usub::pg::PgNotificationListener<MyNotifyHandler> listener("events", conn);
listener.setHandler(MyNotifyHandler{});

co_await listener.run();
```

---

## Behavior

When `run()` starts:

1. Executes `LISTEN <channel>;`
2. If successful, enters an infinite asynchronous loop:

    * Waits for socket readiness using `wait_readable_for_listener()`
    * Calls `PQconsumeInput()` to fetch messages
    * Calls `PQnotifies()` repeatedly to drain all pending notifications
    * For each notification, calls `dispatch_async(handler)`

If connection loss or protocol error occurs, the loop terminates cleanly.

---

## Example: reactive logic

```cpp
struct BalanceUpdateHandler
{
    usub::uvent::task::Awaitable<void>
    operator()(std::string channel,
               std::string payload,
               int pid)
    {
        std::cout << "[NOTIFY] " << channel << ": " << payload << "\n";

        // Reactively query database
        auto& pool = usub::pg::PgPool::instance();
        auto res = co_await pool.query_awaitable(
            "SELECT id, balance FROM users WHERE id = $1;",
            std::stoi(payload)
        );

        if (!res.ok)
            std::cout << "Reactive fetch failed: " << res.error << "\n";
        else
            std::cout << "Updated balance = " << res.rows[0].cols[1] << "\n";

        co_return;
    }
};
```

---

## Dedicated connection

Listeners **must** hold a dedicated connection.
A `LISTEN` session cannot be multiplexed with normal queries.

Always obtain and keep a connection manually:

```cpp
auto conn = co_await pool.acquire_connection();
```

Never use `query_awaitable()` inside the same connection — use the pool’s singleton for secondary queries.

---

## Error transparency (since v1.0.1)

All listener failures now surface as structured logs with `PgErrorCode` mapping.

| Type               | Example                                                                     |
|--------------------|-----------------------------------------------------------------------------|
| Connection invalid | `[NOTIFY][ERROR] code=2 msg=connection invalid at start`                    |
| LISTEN failed      | `[NOTIFY][ERROR] LISTEN failed code=8 sqlstate=42501 msg=permission denied` |
| Socket read failed | `[NOTIFY][WARN] code=3 msg=PQconsumeInput failed: connection reset`         |
| Connection closed  | `[NOTIFY][ERROR] code=2 msg=CONNECTION_BAD`                                 |
| No handler         | `[NOTIFY][INFO] channel=events payload=... pid=1234 (no handler set)`       |

Each code corresponds to a `PgErrorCode` value:

* `ConnectionClosed` — underlying PGconn invalid or socket dropped
* `ServerError` — PostgreSQL rejected the `LISTEN` command
* `SocketReadFailed` — I/O failure during `PQconsumeInput`
* `Unknown` — unexpected condition

These logs make listener health observable and simplify restart logic in supervising coroutines.

---

## Concurrency and safety

* Multiple listeners can run concurrently on different channels.
* Each listener owns exactly one connection.
* Handler invocations are isolated coroutines — slow handlers do not block new notifications.
* Handlers can run nested queries through the global pool safely.

---

## Recovery pattern

You can detect listener termination and restart it automatically:

```cpp
usub::uvent::task::Awaitable<void> run_forever()
{
    while (true)
    {
        auto& pool = usub::pg::PgPool::instance();
        auto conn = co_await pool.acquire_connection();
        if (!conn || !conn->connected()) {
            co_await usub::uvent::system::this_coroutine::sleep_for(1s);
            continue;
        }

        usub::pg::PgNotificationListener<MyNotifyHandler> listener("events", conn);
        listener.setHandler(MyNotifyHandler{});
        co_await listener.run();

        pool.release_connection(conn);
        std::cout << "Listener stopped — restarting\n";
    }
}
```

This provides resilience against connection loss or PostgreSQL restarts.

---

## Notes & limitations

* One listener == one channel.
  To listen on multiple channels, spawn multiple listeners.

* No automatic reconnection — you control lifecycle.

* Handler copies are spawned per event; keep captures small and stateless.

* If your handler throws, the coroutine terminates silently — prefer returning error via logs or metrics.

---

## Design philosophy

`PgNotificationListener` aims for:

* **Zero polling:** pure event-driven design.
* **Low latency:** direct socket wake-ups via `uvent`.
* **No thread blocking:** fully coroutine-based I/O.
* **Transparent errors:** no hidden disconnects or silent failures.

This makes it suitable for reactive systems, message routers, or cross-service synchronization built on PostgreSQL.