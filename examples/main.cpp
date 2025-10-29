#include <chrono>
#include <iostream>
#include "uvent/Uvent.h"
#include "upq/PgPool.h"
#include "upq/PgTransaction.h"
#include "upq/PgNotificationListener.h"
#include "upq/PgNotificationMultiplexer.h"

using namespace usub::uvent;

task::Awaitable<void> test_db_query()
{
    {
        auto res_schema = co_await usub::pg::PgPool::instance().query_awaitable(
            "CREATE TABLE IF NOT EXISTS public.users("
            "id SERIAL PRIMARY KEY,"
            "name TEXT,"
            "password TEXT"
            ");"
        );

        if (!res_schema.ok)
        {
            std::cout << "[ERROR] SCHEMA INIT failed: " << res_schema.error << std::endl;
            co_return;
        }
    }

    {
        usub::pg::PgTransaction txn;

        bool ok_begin = co_await txn.begin();
        if (!ok_begin)
        {
            std::cout << "[ERROR] txn.begin() failed" << std::endl;
            co_await txn.finish();
            co_return;
        }

        {
            auto r_upd = co_await txn.query(
                "UPDATE users SET name = $1 WHERE id = $2 RETURNING name;",
                "John",
                1
            );

            if (!r_upd.ok)
            {
                std::cout << "[ERROR] UPDATE failed: " << r_upd.error << std::endl;
                co_await txn.finish();
                co_return;
            }

            if (!r_upd.rows.empty() && !r_upd.rows[0].cols.empty())
            {
                std::cout << "[INFO] UPDATE ok, new name=" << r_upd.rows[0].cols[0] << std::endl;
            }
            else
            {
                std::cout << "[INFO] UPDATE ok, but no RETURNING rows" << std::endl;
            }
        }

        bool ok_commit = co_await txn.commit();
        if (!ok_commit)
        {
            std::cout << "[ERROR] COMMIT failed" << std::endl;
            co_return;
        }
    }

    {
        auto res_sel = co_await usub::pg::PgPool::instance().query_awaitable(
            "SELECT id, name FROM users ORDER BY id LIMIT $1;",
            5
        );

        if (!res_sel.ok)
        {
            std::cout << "[ERROR] SELECT failed: " << res_sel.error << std::endl;
            co_return;
        }

        if (res_sel.rows.empty())
        {
            std::cout << "[INFO] SELECT returned no rows" << std::endl;
            co_return;
        }

        std::cout << "[INFO] SELECT results:" << std::endl;
        for (auto& row : res_sel.rows)
        {
            if (row.cols.size() >= 2)
                std::cout << "  id=" << row.cols[0] << ", name=" << row.cols[1] << std::endl;
            else
                std::cout << "  incomplete row" << std::endl;
        }
    }

    co_return;
}

struct MyNotifyHandler
{
    usub::uvent::task::Awaitable<void>
    operator()(std::string channel,
               std::string payload,
               int backend_pid) const
    {
        std::cout << "[NOTIFY] ch=" << channel
            << " pid=" << backend_pid
            << " payload=" << payload << std::endl;

        auto& pool = usub::pg::PgPool::instance();
        auto res = co_await pool.query_awaitable(
            "SELECT id, name FROM users WHERE id = $1;",
            1
        );

        if (res.ok && !res.rows.empty())
        {
            std::cout << "reactive fetch -> id=" << res.rows[0].cols[0]
                << ", name=" << res.rows[0].cols[1] << std::endl;
        }
        else
        {
            std::cout << "reactive fetch fail: " << res.error << std::endl;
        }

        co_return;
    }
};

task::Awaitable<void> spawn_listener()
{
    auto& pool = usub::pg::PgPool::instance();

    auto conn = co_await pool.acquire_connection();
    using Listener = usub::pg::PgNotificationListener<MyNotifyHandler>;
    auto listener = std::make_shared<Listener>(
        "events",
        conn
    );

    listener->setHandler(MyNotifyHandler{});

    co_await listener->run();
    co_return;
}

task::Awaitable<void> spawn_listener_multiplexer()
{
    static std::shared_ptr<usub::pg::PgConnectionLibpq> dedicated_conn;
    static std::shared_ptr<usub::pg::PgNotificationMultiplexer<MyNotifyHandler>> mux;

    auto& pool = usub::pg::PgPool::instance();

    if (!dedicated_conn)
    {
        dedicated_conn = std::make_shared<usub::pg::PgConnectionLibpq>();

        std::string conninfo =
            "host=" + pool.host() +
            " port=" + pool.port() +
            " user=" + pool.user() +
            " dbname=" + pool.db() +
            " password=" + pool.password() +
            " sslmode=disable";

        auto err = co_await dedicated_conn->connect_async(conninfo);
        if (err.has_value())
        {
            std::cout << "[FATAL] listener connect failed: " << err.value() << std::endl;
            co_return;
        }

        mux = std::make_shared<usub::pg::PgNotificationMultiplexer<MyNotifyHandler>>(dedicated_conn);

        bool ok1 = co_await mux->add_handler("metrics", MyNotifyHandler{});
        bool ok2 = co_await mux->add_handler("alerts", MyNotifyHandler{});

        if (!ok1 || !ok2)
        {
            std::cout << "[FATAL] LISTEN failed\n";
            co_return;
        }

        usub::uvent::system::co_spawn(mux->run());
    }

    co_return;
}

int main()
{
    settings::timeout_duration_ms = 5000;

    usub::Uvent uvent(4);

    usub::pg::PgPool::init_global(
        "localhost", // host
        "12432", // port
        "postgres", // user
        "postgres", // db
        "password", // password
        /*max_pool_size*/ 32,
        /*queue_capacity*/ 64
    );

    uvent.for_each_thread([&](int threadIndex, thread::ThreadLocalStorage* tls)
    {
        system::co_spawn_static(
            test_db_query(),
            threadIndex
        );

        system::co_spawn_static(spawn_listener_multiplexer(), threadIndex);
    });

    usub::uvent::system::co_spawn(spawn_listener());

    uvent.run();
    return 0;
}
