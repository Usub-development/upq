# PgNotificationMultiplexer

`PgNotificationMultiplexer` allows you to handle multiple PostgreSQL `LISTEN` channels concurrently using a single
dedicated connection.

It multiplexes incoming notifications (`PGnotify`) and dispatches them asynchronously to all registered handlers.

---

## Overview

Unlike `PgNotificationListener`, which listens to a single channel per connection,  
`PgNotificationMultiplexer` maintains a registry of channels → handlers on one connection.

Each notification received by PostgreSQL is automatically routed to all handlers registered for its channel.

---

## Usage Example

```cpp
#include "upq/PgNotificationMultiplexer.h"

struct MyNotifyHandler
{
    usub::uvent::task::Awaitable<void>
    operator()(std::string channel, std::string payload, int backend_pid) const
    {
        std::cout << "[NOTIFY] " << channel
                  << " payload=" << payload
                  << " pid=" << backend_pid << std::endl;
        co_return;
    }
};

task::Awaitable<void> start_mux()
{
    using namespace usub::pg;

    auto dedicated = std::make_shared<PgConnectionLibpq>();
    std::string conninfo = "host=localhost port=5432 user=postgres dbname=postgres password=secret";

    auto err = co_await dedicated->connect_async(conninfo);
    if (err)
    {
        std::cout << "connect failed: " << *err << std::endl;
        co_return;
    }

    auto mux = std::make_shared<PgNotificationMultiplexer<MyNotifyHandler>>(dedicated);

    bool ok1 = co_await mux->add_handler("metrics", MyNotifyHandler{});
    bool ok2 = co_await mux->add_handler("alerts", MyNotifyHandler{});

    if (!ok1 || !ok2)
    {
        std::cout << "LISTEN failed\n";
        co_return;
    }

    usub::uvent::system::co_spawn(mux->run());
    co_return;
}
```

---

## API

### Template Parameter

`HandlerT` — a coroutine functor matching:

```cpp
usub::uvent::task::Awaitable<void>
operator()(std::string channel,
           std::string payload,
           int backend_pid);
```

### Public Interface

| Method                                                               | Description                                                                                                             |
|----------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| `PgNotificationMultiplexer(std::shared_ptr<PgConnectionLibpq> conn)` | Constructs a multiplexer bound to a specific connection                                                                 |
| `Awaitable<bool> add_handler(std::string channel, HandlerT handler)` | Registers a handler for a given channel; executes `LISTEN <channel>` if not yet registered                              |
| `Awaitable<void> run()`                                              | Starts a perpetual coroutine loop that consumes PostgreSQL notifications and dispatches them asynchronously to handlers |

---

## Design Notes

* Each channel can have multiple handlers; each handler is invoked in its own coroutine via `co_spawn`.
* The multiplexer connection must remain alive; it should **not** be part of the general pool.
* Handlers execute fully asynchronously; their execution never blocks the event loop reading notifications.
* Thread-safe if used within a single `uvent` loop.
* Ideal for runtime systems reacting to PostgreSQL events (`metrics`, `alerts`, `jobs`, etc.).