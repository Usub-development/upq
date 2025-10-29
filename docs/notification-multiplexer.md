# PgNotificationMultiplexer

`PgNotificationMultiplexer` listens to many PostgreSQL `LISTEN` channels on **one** dedicated connection and dispatches
notifications to registered handlers asynchronously. Supports wildcards, per-channel queues, rate limiting, and
auto-reconnect with resubscribe.

---

## Key features

- **One connection, many channels** (exact and wildcard `prefix.*`)
- **Multiple handlers per channel**
- **Per-channel MPMC queues** with capacity limits
- **Rate limiting** per channel (`rate_limit_per_sec`)
- **Auto-reconnect** with exponential polling and **resubscribe**
- **Pending buffer** for events seen during disconnect
- **Recursion guard** to avoid handler self-loops
- **Drop counters**: overflow / recursion / rate-limit

---

## Quick start

```cpp
#include "upq/PgNotificationMultiplexer.h"
using namespace usub::pg;

// 1) Implement a handler
struct MyHandler final : IPgNotifyHandler {
    usub::uvent::task::Awaitable<void>
    operator()(std::string channel, std::string payload, int backend_pid) override
    {
        std::cout << "[NOTIFY] ch=" << channel
                  << " pid=" << backend_pid
                  << " payload=" << payload << "\n";
        co_return;
    }
};

usub::uvent::task::Awaitable<void> start_mux()
{
    // 2) Prepare a dedicated connection (do NOT put it back to the pool)
    auto conn = std::make_shared<PgConnectionLibpq>();
    auto err = co_await conn->connect_async(
        "host=127.0.0.1 port=5432 user=postgres dbname=app password=secret sslmode=disable"
    );
    if (err) co_return; // handle error yourself

    // 3) Create multiplexer (also pass creds for reconnects)
    PgNotificationMultiplexer mux{
        conn, "127.0.0.1", "5432", "postgres", "app", "secret",
        PgNotificationMultiplexer::Config{
            /*channel_queue_capacity*/             1024,
            /*pending_after_disconnect_capacity*/  2048,
            /*reconnect_backoff_us*/               100'000,
            /*max_recursive_depth*/                4,
            /*rate_limit_per_sec*/                 10'000
        }
    };

    // 4) Register handlers
    auto h_metrics = co_await mux.add_handler("metrics", std::make_shared<MyHandler>());
    auto h_alerts  = co_await mux.add_handler("alerts",  std::make_shared<MyHandler>());
    auto h_any_pay = co_await mux.add_handler("payments.*", std::make_shared<MyHandler>()); // wildcard

    if (!h_metrics || !h_alerts || !h_any_pay) co_return; // LISTEN failed

    // 5) Run forever (spawn in your loop)
    usub::uvent::system::co_spawn(mux.run());
    co_return;
}
```

---

## Wildcards

* Use `prefix.*` (must end with `.*`).
* Example: handler on `payments.*` will receive `payments.created`, `payments.updated`, etc.
* Exact and wildcard handlers can coexist.

---

## Backpressure & drops

Each channel has its own queue with fixed capacity. When full, events are dropped and counted:

* `dropped_overflow` — queue full
* `dropped_rate_limited` — exceeded `rate_limit_per_sec`
* `dropped_recursive` — recursion guard blocked the event

Read aggregate counters:

```cpp
PgNotificationMultiplexer::Stats s = mux.stats();
std::cout << "overflow=" << s.dropped_overflow
          << " recursive=" << s.dropped_recursive
          << " rate_limited=" << s.dropped_rate_limited << "\n";
```

---

## Reconnect behavior

* On disconnect, the mux retries `connect_async` using provided host/port/user/db/password with `sslmode=disable`.
* After reconnect it **resubscribes** to all exact channels and **flushes** buffered pending events.
* Wildcard registrations are kept in memory and continue to match.

---

## Recursion guard

Per-thread guard prevents infinite loops if handlers `NOTIFY` back the same `(channel,payload)` repeatedly.
Depth limit is `max_recursive_depth`. Beyond that, identical `(channel,payload)` are dropped and counted as
`dropped_recursive`.

---

## API

### Types

```cpp
struct IPgNotifyHandler {
    virtual usub::uvent::task::Awaitable<void>
    operator()(std::string channel, std::string payload, int backend_pid) = 0;
    virtual ~IPgNotifyHandler() = default;
};

struct PgNotificationMultiplexer::Config {
    size_t   channel_queue_capacity              = 256;
    size_t   pending_after_disconnect_capacity   = 1024;
    uint64_t reconnect_backoff_us                = 100'000;
    uint32_t max_recursive_depth                 = 4;
    uint32_t rate_limit_per_sec                  = 1000;
};

struct PgNotificationMultiplexer::HandlerHandle {
    uint64_t    id;
    std::string channel;   // exact or wildcard
    bool        wildcard;
};

struct PgNotificationMultiplexer::Stats {
    uint64_t dropped_overflow;
    uint64_t dropped_recursive;
    uint64_t dropped_rate_limited;
};
```

### Constructor

```cpp
PgNotificationMultiplexer(
    std::shared_ptr<PgConnectionLibpq> conn,
    std::string host, std::string port,
    std::string user, std::string db,
    std::string password,
    Config cfg = {}
);
```

> The initial `conn` is used immediately; credentials are stored for reconnects.

### Methods

| Method                                                                                                                                                                                                                       |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `task::Awaitable<std::optional<HandlerHandle>> add_handler(const std::string& channel, std::shared_ptr<IPgNotifyHandler> handler)` — Registers handler; `LISTEN` for exact channels; returns handle or `nullopt` on failure. |
| `bool remove_handler(const HandlerHandle& h)` — Unregisters a specific handler; for exact channels, does `UNLISTEN` and tears down the channel when the last handler is removed.                                             |
| `bool remove_channel(const std::string& channel)` — Removes all handlers for the channel; exact channels are `UNLISTEN`ed.                                                                                                   |
| `task::Awaitable<void> run()` — Main loop; consumes `PGnotify`, routes to per-channel workers, handles reconnects.                                                                                                           |
| `Stats stats() const` — Aggregate drop counters across all channels.                                                                                                                                                         |

---

## Handler execution model

* Delivery order per channel is FIFO by queueing point.
* Each handler call is `co_spawn`’d; slow handlers don’t block the reader.
* Handlers **must** be resilient (no throws; handle their own failures).

---

## When to dedicate the connection

Always. Don’t share the mux connection with general query load or return it to the pool. Keep it alive for the entire
process lifetime.