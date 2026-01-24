#include <chrono>
#include <iostream>
#include <string>

#include "uvent/Uvent.h"
#include "upq/PgConnection.h"
#include "upq/PgPool.h"
#include "upq/PgTransaction.h"

using namespace usub::uvent;

static void log_ts(const char *msg) {
    using namespace std::chrono;
    auto now = steady_clock::now().time_since_epoch();
    auto ms = duration_cast<milliseconds>(now).count();
    std::cout << "[" << ms << " ms] " << msg << std::endl;
}

task::Awaitable<void> connect_with_timeout() {
    using namespace std::chrono_literals;

    log_ts("connect_with_timeout(): start");

    usub::pg::PgConnectionLibpq conn;

    std::string conninfo =
            "host=10.255.255.1 "
            "port=5432 "
            "dbname=postgres "
            "user=postgres "
            "password=postgres";

    auto t0 = std::chrono::steady_clock::now();

    auto err = co_await conn.connect_async(conninfo, 3s);

    auto t1 = std::chrono::steady_clock::now();
    auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    if (err) {
        std::cout << "[CONNECT] failed after " << elapsed_ms
                << " ms: " << *err << std::endl;
    } else {
        std::cout << "[CONNECT] unexpectedly succeeded after "
                << elapsed_ms << " ms" << std::endl;
    }

    log_ts("connect_with_timeout(): done");
    co_return;
}

task::Awaitable<void> tx_example() {
    using namespace std::chrono_literals;

    log_ts("tx_example(): start");

    usub::pg::PgPool pool(
        "localhost", // host
        "12432", // port
        "postgres", // user
        "postgres", // db
        "password", // password
        8 // pool size
    ); {
        auto r = co_await pool.query_awaitable(R"SQL(
            CREATE TABLE IF NOT EXISTS tx_demo (
                id   BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL
            );
        )SQL");
        if (!r.ok) {
            std::cout << "[TX/SCHEMA] " << r.error << "\n";
            co_return;
        }
    }

    usub::pg::PgTransaction tx(&pool);
    if (auto err_begin = co_await tx.begin_errored(); err_begin) {
        co_await tx.finish();
        const auto &e = err_begin.value();

        std::cout << "[TX] begin failed" << toString(e.code) << ", " << e.error << ", " << e.err_detail.sqlstate << ", "
        << e.err_detail.message;
        co_return;
    } {
        const char *timeout = "2s";
        auto setr = co_await tx.query("SELECT set_config('lock_timeout', $1, true);", timeout);
        if (!setr.ok)
            std::cout << "[TX/SET LOCAL] " << setr.error << "\n";
    }

    std::string inserted_id; {
        auto ins = co_await tx.query(
            "INSERT INTO tx_demo(name) VALUES ($1) RETURNING id;",
            "from_tx_example"
        );

        if (!ins.ok) {
            std::cout << "[TX/INSERT] " << ins.error << "\n";
            co_await tx.rollback();
            co_return;
        }

        if (!ins.rows.empty() && !ins.rows[0].cols.empty()) {
            inserted_id = ins.rows[0].cols[0];
            std::cout << "[TX/INSERT] id=" << inserted_id << "\n";
        } else {
            std::cout << "[TX/INSERT] no RETURNING row\n";
        }
    }

    if (!inserted_id.empty()) {
        auto upd = co_await tx.query(
            "UPDATE tx_demo SET name = $1 WHERE id = $2",
            "updated_in_tx",
            inserted_id
        );

        if (!upd.ok) {
            std::cout << "[TX/UPDATE] " << upd.error << "\n";
            co_await tx.rollback();
            co_return;
        }

        std::cout << "[TX/UPDATE] affected=" << upd.rows_affected << "\n";
    }

    if (!(co_await tx.commit())) {
        std::cout << "[TX] commit failed\n";
        co_return;
    } {
        auto sel = co_await pool.query_awaitable(
            "SELECT id, name FROM tx_demo ORDER BY id DESC LIMIT 1;"
        );

        if (!sel.ok) {
            std::cout << "[TX/SELECT] " << sel.error << "\n";
        } else if (!sel.rows.empty() && sel.rows[0].cols.size() >= 2) {
            std::cout << "[TX/SELECT] id=" << sel.rows[0].cols[0]
                    << " name=" << sel.rows[0].cols[1] << "\n";
        } else {
            std::cout << "[TX/SELECT] no rows\n";
        }
    }

    log_ts("tx_example(): done");
    co_return;
}

int main() {
    log_ts("main(): before Uvent");

    usub::Uvent uvent(1);

    log_ts("main(): before co_spawn");

    usub::uvent::system::co_spawn(connect_with_timeout());

    usub::uvent::system::co_spawn(tx_example());

    log_ts("main(): before run()");

    uvent.run();

    log_ts("main(): after run()");
    return 0;
}
