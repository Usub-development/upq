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

task::Awaitable<void> massive_ops_example()
{
    auto& pool = usub::pg::PgPool::instance();

    {
        auto res_schema = co_await pool.query_awaitable(
            "CREATE TABLE IF NOT EXISTS public.bigdata("
            "id BIGSERIAL PRIMARY KEY,"
            "payload TEXT"
            ");"
        );

        if (!res_schema.ok)
        {
            std::cout << "[ERROR] bigdata schema init failed: " << res_schema.error << std::endl;
            co_return;
        }
    }

    {
        auto conn = co_await pool.acquire_connection();
        if (!conn || !conn->connected())
        {
            std::cout << "[ERROR] no conn for COPY IN" << std::endl;
            co_return;
        }

        {
            usub::pg::PgCopyResult st = co_await conn->copy_in_start(
                "COPY public.bigdata(payload) FROM STDIN"
            );

            if (!st.ok)
            {
                std::cout << "[ERROR] COPY IN start failed: " << st.error << std::endl;
                co_await pool.release_connection_async(conn);
                co_return;
            }
        }

        {
            for (int i = 0; i < 5; i++)
            {
                std::string line = "payload line " + std::to_string(i) + "\n";

                usub::pg::PgCopyResult chunk_res = co_await conn->copy_in_send_chunk(
                    line.data(),
                    line.size()
                );

                if (!chunk_res.ok)
                {
                    std::cout << "[ERROR] COPY IN chunk failed: " << chunk_res.error << std::endl;
                    co_await pool.release_connection_async(conn);
                    co_return;
                }
            }
        }

        {
            usub::pg::PgCopyResult fin = co_await conn->copy_in_finish();
            if (!fin.ok)
            {
                std::cout << "[ERROR] COPY IN finish failed: " << fin.error << std::endl;
                co_await pool.release_connection_async(conn);
                co_return;
            }

            std::cout << "[INFO] COPY IN done, rows_affected=" << fin.rows_affected << std::endl;
        }

        co_await pool.release_connection_async(conn);
    }

    {
        auto conn = co_await pool.acquire_connection();
        if (!conn || !conn->connected())
        {
            std::cout << "[ERROR] no conn for COPY OUT" << std::endl;
            co_return;
        }

        {
            usub::pg::PgCopyResult st = co_await conn->copy_out_start(
                "COPY (SELECT id, payload FROM public.bigdata ORDER BY id LIMIT 10) TO STDOUT"
            );

            if (!st.ok)
            {
                std::cout << "[ERROR] COPY OUT start failed: " << st.error << std::endl;
                co_await pool.release_connection_async(conn);
                co_return;
            }
        }

        while (true)
        {
            auto chunk = co_await conn->copy_out_read_chunk();
            if (!chunk.ok)
            {
                std::cout << "[ERROR] COPY OUT chunk read failed: " << chunk.err.message << std::endl;
                break;
            }

            if (chunk.value.empty())
            {
                std::cout << "[INFO] COPY OUT finished" << std::endl;
                break;
            }

            std::string s(chunk.value.begin(), chunk.value.end());
            std::cout << "[COPY-OUT-CHUNK] " << s;
        }

        co_await pool.release_connection_async(conn);
    }

    {
        auto conn = co_await pool.acquire_connection();
        if (!conn || !conn->connected())
        {
            std::cout << "[ERROR] no conn for cursor" << std::endl;
            co_return;
        }

        std::string cursor_name = conn->make_cursor_name();

        {
            auto decl_res = co_await conn->cursor_declare(
                cursor_name,
                "SELECT id, payload FROM public.bigdata ORDER BY id"
            );

            if (!decl_res.ok)
            {
                std::cout << "[ERROR] cursor DECLARE failed: " << decl_res.error << std::endl;
                co_await pool.release_connection_async(conn);
                co_return;
            }
        }

        while (true)
        {
            usub::pg::PgCursorChunk ck = co_await conn->cursor_fetch_chunk(cursor_name, 3);

            if (!ck.ok)
            {
                std::cout << "[ERROR] cursor FETCH failed: " << ck.error << std::endl;
                break;
            }

            if (ck.rows.empty())
            {
                std::cout << "[INFO] cursor FETCH done" << std::endl;
                break;
            }

            for (auto& row : ck.rows)
            {
                if (row.cols.size() >= 2)
                {
                    std::cout << "[CURSOR] id=" << row.cols[0]
                              << " payload=" << row.cols[1] << std::endl;
                }
                else
                {
                    std::cout << "[CURSOR] incomplete row" << std::endl;
                }
            }

            if (ck.done)
            {
                std::cout << "[INFO] cursor reported done" << std::endl;
                break;
            }
        }

        {
            auto cls_res = co_await conn->cursor_close(cursor_name);
            if (!cls_res.ok)
            {
                std::cout << "[WARN] cursor CLOSE failed: " << cls_res.error << std::endl;
            }
            else
            {
                std::cout << "[INFO] cursor closed" << std::endl;
            }
        }

        pool.release_connection(conn);
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
        /*queue_capacity*/ 64,
        usub::pg::PgPoolHealthConfig{
            .enabled = true,
            .interval_ms = 3000
        }
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
    usub::uvent::system::co_spawn(massive_ops_example());

    uvent.run();
    return 0;
}
