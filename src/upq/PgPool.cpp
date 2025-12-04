#include "upq/PgPool.h"
#include <cstdlib>

namespace usub::pg
{
    PgPool::PgPool(std::string host,
                   std::string port,
                   std::string user,
                   std::string db,
                   std::string password,
                   size_t max_pool_size, int retries_on_connection_failed)
        : host_(std::move(host))
          , port_(std::move(port))
          , user_(std::move(user))
          , db_(std::move(db))
          , password_(std::move(password))
          , idle_(max_pool_size)
          , max_pool_(max_pool_size)
          , live_count_(0)
          , stats_{}
          , retries_on_connection_failed_(retries_on_connection_failed)
    {
    }

    PgPool::~PgPool() = default;

    usub::uvent::task::Awaitable<std::expected<std::shared_ptr<PgConnectionLibpq>, PgOpError>>
    PgPool::acquire_connection()
    {
        using namespace std::chrono_literals;

        std::shared_ptr<PgConnectionLibpq> conn;

        for (int i = 0; i < retries_on_connection_failed_; ++i)
        {
            if (this->idle_.try_dequeue(conn))
            {
                if (!conn || !conn->connected() || !conn->is_idle())
                {
                    mark_dead(conn);
                    continue;
                }

                co_return std::expected<std::shared_ptr<PgConnectionLibpq>, PgOpError>{
                    std::in_place, std::move(conn)
                };
            }

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
                        mark_dead(newConn);
                    }
                    else
                    {
                        co_return std::expected<std::shared_ptr<PgConnectionLibpq>, PgOpError>{
                            std::in_place, std::move(newConn)
                        };
                    }
                }
            }

            co_await usub::uvent::system::this_coroutine::sleep_for(100us);
        }

        co_return std::unexpected(PgOpError{
            PgErrorCode::TooManyConnections,
            "Too many opened connections or connection failed after retries",
            {}
        });
    }

    void PgPool::release_connection(std::shared_ptr<PgConnectionLibpq> conn)
    {
        if (!conn)
            return;

        if (!conn->connected() || !conn->is_idle())
        {
            mark_dead(conn);
            return;
        }

        if (!this->idle_.try_enqueue(conn))
        {
            mark_dead(conn);
        }
    }

    usub::uvent::task::Awaitable<void>
    PgPool::release_connection_async(std::shared_ptr<PgConnectionLibpq> conn)
    {
        if (!conn)
        {
            co_return;
        }

        if (!conn->connected())
        {
            mark_dead(conn);
            co_return;
        }

        bool pumped = co_await conn->pump_input();
        if (pumped)
        {
            (void)conn->drain_all_results();
        }

        if (!conn->connected() || !conn->is_idle())
        {
            mark_dead(conn);
            co_return;
        }

        if (!this->idle_.try_enqueue(conn))
        {
            mark_dead(conn);
        }

        co_return;
    }

    void PgPool::mark_dead(std::shared_ptr<PgConnectionLibpq> const& conn)
    {
        if (!conn)
            return;

        conn->close();
        this->live_count_.fetch_sub(1, std::memory_order_relaxed);
    }
}
