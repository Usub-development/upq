# Notifications (LISTEN / NOTIFY)

`PgNotificationListener` subscribes to a PostgreSQL channel using `LISTEN <channel>` and delivers `NOTIFY` messages to your handler as coroutines.  
This gives you push-style DB events without polling.

This is useful for:
- cache invalidation
- balance updates / ledgers
- cross-process signals ("wake worker", "reload config", etc.)
- async domain events

---

## Integration with webserver based on uvent can be found [here](https://github.com/Usub-development/webserver/tree/uvent302/examples/upq_integration).

## Overview

```cpp
template <class HandlerT>
    requires PgNotifyHandler<HandlerT>
class PgNotificationListener
{
public:
    PgNotificationListener(std::string channel,
                           std::shared_ptr<PgConnectionLibpq> conn);

    void setHandler(HandlerT h);

    usub::uvent::task::Awaitable<void> run();
};
```

High-level flow:

1. You grab a dedicated PostgreSQL connection from `PgPool`.
2. You create `PgNotificationListener(channel, conn)`.
3. You give it a handler.
4. You `co_await run()`.
5. It blocks (asynchronously) and keeps dispatching notifications.

The listener is single-channel. One listener == one `LISTEN <channel>`.

---

## Handler contract

Your handler type must provide a call operator with this exact shape:

```cpp
usub::uvent::task::Awaitable<void>
operator()(std::string channel,
           std::string payload,
           int backend_pid);
```

* `channel`: channel name from PostgreSQL `NOTIFY`.
* `payload`: the `NOTIFY` payload string.
* `backend_pid`: PID of the PostgreSQL backend that sent the message.

The handler is awaited as a coroutine. Each notification is scheduled via `co_spawn`, so notifications are handled asynchronously and independently.

### Example handler

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
            << " payload=" << payload
            << std::endl;

        // You can run DB queries here if you want to react.
        auto& pool = usub::pg::PgPool::instance();

        auto res = co_await pool.query_awaitable(
            "SELECT id, name FROM users WHERE id = $1;",
            1
        );

        if (res.ok && !res.rows.empty())
        {
            std::cout
                << "reactive fetch -> id="   << res.rows[0].cols[0]
                << ", name="                 << res.rows[0].cols[1]
                << std::endl;
        }
        else
        {
            std::cout
                << "reactive fetch fail: "
                << res.error
                << std::endl;
        }

        co_return;
    }
};
```

This satisfies the `PgNotifyHandler` concept and matches how the listener actually calls the handler.

---

## Creating and running a listener

### 1. Get a dedicated connection from the pool

You should not use a "borrowed for one query" connection here. You keep this connection for as long as you're listening.

```cpp
auto& pool = usub::pg::PgPool::instance();
auto conn = co_await pool.acquire_connection();

if (!conn || !conn->connected())
{
    std::cout << "listener: connection unavailable\n";
    co_return;
}
```

Why dedicated?
Because this connection will sit in a loop waiting for notifications. You don't want to return it to the pool between events.

---

### 2. Create the listener and attach the handler

```cpp
usub::pg::PgNotificationListener<MyNotifyHandler> listener(
    "events",  // PostgreSQL channel name
    conn       // shared_ptr<PgConnectionLibpq>
);

listener.setHandler(MyNotifyHandler{});
```

At runtime this will execute:

```sql
LISTEN events;
```

If that initial `LISTEN` fails, the listener stops immediately.

---

### 3. Start the loop

```cpp
co_await listener.run();
```

`run()`:

1. Executes `LISTEN <channel>;`
2. Enters an infinite async loop:
    * waits for the socket to become readable using `wait_readable_for_listener()`
    * calls `PQconsumeInput(raw_conn)` to pull pending messages from the server
    * repeatedly calls `PQnotifies(raw_conn)` to drain all notifications
    * for each notification, schedules your handler via `co_spawn`

When the connection becomes invalid or `PQconsumeInput()` reports `CONNECTION_BAD`, `run()` returns and the listener stops.

---

## Full example

A realistic bootstrap coroutine that subscribes to `"events"` and runs forever:

```cpp
usub::uvent::task::Awaitable<void> start_notifications()
{
    auto& pool = usub::pg::PgPool::instance();
    auto conn = co_await pool.acquire_connection();

    if (!conn || !conn->connected())
    {
        std::cout << "cannot init notification listener\n";
        co_return;
    }

    usub::pg::PgNotificationListener<MyNotifyHandler> listener(
        "events",
        conn
    );

    listener.setHandler(MyNotifyHandler{});

    // This call will not return under normal operation.
    // It returns only if the connection dies or becomes invalid.
    co_await listener.run();

    // If run() exits, you can optionally release the connection:
    pool.release_connection(conn);

    co_return;
}
```

You typically spawn this once during service startup:

```cpp
usub::uvent::system::co_spawn(
    start_notifications()
);
```

After that, any `NOTIFY events, 'payload'` from Postgres will trigger `MyNotifyHandler`.

---

## Internals / behavior details

Below is what `PgNotificationListener` actually does (simplified from the code you provided):

```cpp
usub::uvent::task::Awaitable<void> run()
{
    if (!conn_ || !conn_->connected())
        co_return;

    // Step 1: LISTEN <channel>;
    {
        std::string listen_sql = "LISTEN " + channel_ + ";";
        QueryResult qr = co_await conn_->exec_simple_query_nonblocking(listen_sql);
        if (!qr.ok)
            co_return; // can't subscribe, stop
    }

    // Step 2: main loop
    while (true)
    {
        // block (asynchronously) until socket is readable
        co_await conn_->wait_readable_for_listener();

        PGconn* raw = conn_->raw_conn();
        if (!raw)
            co_return;

        // pull data from libpq
        if (PQconsumeInput(raw) == 0)
        {
            // check if connection is broken
            if (PQstatus(raw) == CONNECTION_BAD)
                co_return;

            continue;
        }

        // drain all pending notifications
        while (true)
        {
            PGnotify* n = PQnotifies(raw);
            if (!n)
                break;

            const char* ch_raw = n->relname ? n->relname : "";
            const char* pl_raw = n->extra  ? n->extra  : "";
            int be_pid = n->be_pid;

            if (has_handler_)
                dispatch_async(ch_raw, pl_raw, be_pid);

            PQfreemem(n);
        }
    }

    co_return;
}
```

Key points:

* It's event-driven. No polling delays, no sleep.
* It never blocks the thread. Waiting is done via `uvent` (`wait_readable_for_listener()`).
* Every notification is handed off using `dispatch_async(...)`, which internally does `co_spawn` on a new coroutine so that handler logic doesn't backpressure the listener.

---

## Notes / limitations

* One listener listens to exactly one channel string given in constructor.
  If you want multiple channels: make multiple listeners, or extend the code to run more than one `LISTEN ...;`.

* There is no automatic reconnect / re-LISTEN if the connection dies. You own the lifecycle. Usual pattern is:

    * detect exit of `run()`
    * log it
    * spawn a new listener with a fresh connection if you care about resilience

* The listener assumes the same `PGconn` is held for its entire lifetime. Do not `release_connection()` while `run()` is active.

* Handler is copied before dispatch. That means:

    * captures need to be cheap or at least copyable,
    * you can safely mutate shared state via globals/singletons (like `PgPool::instance()`), but don't rely on handler object mutating its own internal state across calls unless you understand you're mutating *the copy*.

---

## TL;DR

* Grab connection from pool
* Build `PgNotificationListener("channel", conn)`
* `setHandler(MyNotifyHandler{})`
* `co_await listener.run()` in a background coroutine
* Every `NOTIFY channel, 'payload'` in Postgres hits your handler as its own coroutine