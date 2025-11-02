#include "upq/PgPool.h"
#include <cstdlib>

namespace usub::pg
{
    PgPool::PgPool(std::string host,
                   std::string port,
                   std::string user,
                   std::string db,
                   std::string password,
                   size_t max_pool_size)
        : host_(std::move(host))
          , port_(std::move(port))
          , user_(std::move(user))
          , db_(std::move(db))
          , password_(std::move(password))
          , idle_(max_pool_size)
          , max_pool_(max_pool_size)
          , live_count_(0)
          , stats_{}
    {
    }

    PgPool::~PgPool() = default;

    usub::uvent::task::Awaitable<std::shared_ptr<PgConnectionLibpq>>
    PgPool::acquire_connection()
    {
        using namespace std::chrono_literals;

        std::shared_ptr<PgConnectionLibpq> conn;

        for (;;)
        {
            // Try to reuse an idle connection first.
            if (this->idle_.try_dequeue(conn))
            {
                if (!conn->connected())
                {
                    // Retire broken connection from pool counters.
                    this->live_count_.fetch_sub(1, std::memory_order_relaxed);
                    continue;
                }
                co_return conn;
            }

            // Create a new connection if we are under the limit.
            size_t cur_live = this->live_count_.load(std::memory_order_relaxed);
            if (cur_live < this->max_pool_)
            {
                if (this->live_count_.compare_exchange_strong(
                    cur_live,
                    cur_live + 1,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed))
                {
                    auto newConn = std::make_shared<PgConnectionLibpq>();

                    std::string conninfo =
                        "host=" + this->host_ +
                        " port=" + this->port_ +
                        " user=" + this->user_ +
                        " dbname=" + this->db_ +
                        " password=" + this->password_ +
                        " sslmode=disable";

                    auto err = co_await newConn->connect_async(conninfo);
                    if (err.has_value())
                    {
                        // Failed to establish; roll back live counter.
                        this->live_count_.fetch_sub(1, std::memory_order_relaxed);
                    }
                    else
                    {
                        co_return newConn;
                    }
                }
            }

            // Back off briefly before retrying.
            co_await usub::uvent::system::this_coroutine::sleep_for(100us);
        }
    }

    void PgPool::release_connection(std::shared_ptr<PgConnectionLibpq> conn)
    {
        if (!conn)
            return;

        // Retire broken connections.
        if (!conn->connected())
        {
            this->live_count_.fetch_sub(1, std::memory_order_relaxed);
            return;
        }

        // If the connection is not clean, retire it.
        if (!conn->is_idle())
        {
            this->live_count_.fetch_sub(1, std::memory_order_relaxed);
            return;
        }

        // Recycle if queue has space; else retire.
        if (!this->idle_.try_enqueue(conn))
        {
            this->live_count_.fetch_sub(1, std::memory_order_relaxed);
        }
    }

    usub::uvent::task::Awaitable<void>
    PgPool::release_connection_async(std::shared_ptr<PgConnectionLibpq> conn)
    {
        if (!conn || !conn->connected())
        {
            this->live_count_.fetch_sub(1, std::memory_order_relaxed);
            co_return;
        }

        // Drain pending results (safe path), then recycle.
        bool pumped = co_await conn->pump_input();
        if (pumped)
        {
            (void)conn->drain_all_results();
        }

        if (!this->idle_.try_enqueue(conn))
        {
            this->live_count_.fetch_sub(1, std::memory_order_relaxed);
        }

        co_return;
    }

    void PgPool::mark_dead(std::shared_ptr<PgConnectionLibpq> const& conn)
    {
        if (!conn)
            return;

        // Simply decrement live counter; connection object will be freed elsewhere.
        this->live_count_.fetch_sub(1, std::memory_order_relaxed);
    }
}
