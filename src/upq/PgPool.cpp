#include "upq/PgPool.h"
#include <cstdlib>

namespace usub::pg
{
    std::unique_ptr<PgPool> PgPool::instance_ = nullptr;

    PgPool::PgPool(std::string host,
                   std::string port,
                   std::string user,
                   std::string db,
                   std::string password,
                   size_t max_pool_size,
                   size_t queue_capacity)
        : host_(std::move(host))
          , port_(std::move(port))
          , user_(std::move(user))
          , db_(std::move(db))
          , password_(std::move(password))
          , idle_(queue_capacity)
          , max_pool_(max_pool_size)
          , live_count_(0)
    {
    }

    void PgPool::init_global(const std::string& host,
                             const std::string& port,
                             const std::string& user,
                             const std::string& db,
                             const std::string& password,
                             size_t max_pool_size,
                             size_t queue_capacity)
    {
        if (instance_)
        {
            return;
        }

        instance_.reset(
            new PgPool(
                host,
                port,
                user,
                db,
                password,
                max_pool_size,
                queue_capacity
            )
        );

    }

    PgPool& PgPool::instance()
    {
        if (!instance_)
        {
            std::abort();
        }
        return *instance_;
    }

    usub::uvent::task::Awaitable<std::shared_ptr<PgConnectionLibpq>>
    PgPool::acquire_connection()
    {
        using namespace std::chrono_literals;

        std::shared_ptr<PgConnectionLibpq> conn;

        for (;;)
        {
            if (this->idle_.try_dequeue(conn))
            {
                if (!conn->connected())
                {
                    this->live_count_.fetch_sub(1, std::memory_order_relaxed);
                    continue;
                }
                co_return conn;
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
                        this->live_count_.fetch_sub(1, std::memory_order_relaxed);
                    }
                    else
                    {
                        co_return newConn;
                    }
                }
            }

            co_await usub::uvent::system::this_coroutine::sleep_for(100us);
        }
    }

    void PgPool::release_connection(std::shared_ptr<PgConnectionLibpq> conn)
    {
        if (!conn)
            return;

        if (!conn->connected())
        {
            this->live_count_.fetch_sub(1, std::memory_order_relaxed);
            return;
        }

        if (!this->idle_.try_enqueue(conn))
        {
            this->live_count_.fetch_sub(1, std::memory_order_relaxed);
        }
    }
} // namespace usub::pg
