#ifndef PGPOOL_H
#define PGPOOL_H

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <atomic>
#include <chrono>

#include "uvent/Uvent.h"

#include "PgConnection.h"
#include "PgTypes.h"
#include "uvent/utils/datastructures/queue/ConcurrentQueues.h"

namespace usub::pg
{
    struct PgPoolHealthConfig
    {
        bool enabled{false};
        uint64_t interval_ms{600000};
    };

    class PgHealthChecker;

    class PgPool
    {
    public:
        PgPool(std::string host,
               std::string port,
               std::string user,
               std::string db,
               std::string password,
               size_t max_pool_size = 32,
               size_t queue_capacity = 64,
               PgPoolHealthConfig health_cfg = {});

        static void init_global(const std::string& host,
                                const std::string& port,
                                const std::string& user,
                                const std::string& db,
                                const std::string& password,
                                size_t max_pool_size = 32,
                                size_t queue_capacity = 64,
                                PgPoolHealthConfig health_cfg = {});

        static PgPool& instance();

        usub::uvent::task::Awaitable<std::shared_ptr<PgConnectionLibpq>>
        acquire_connection();

        void release_connection(std::shared_ptr<PgConnectionLibpq> conn);

        template <typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query_on(std::shared_ptr<PgConnectionLibpq> const& conn,
                 const std::string& sql,
                 Args&&... args);

        template <typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query_awaitable(const std::string& sql, Args&&... args);

        inline std::string host() { return this->host_; }
        inline std::string port() { return this->port_; }
        inline std::string user() { return this->user_; }
        inline std::string db() { return this->db_; }
        inline std::string password() { return this->password_; }

        inline PgPoolHealthConfig health_cfg() const { return this->health_cfg_; }

        PgHealthChecker& health_checker();

        void mark_dead(std::shared_ptr<PgConnectionLibpq> const& conn);

        struct HealthStats
        {
            std::atomic<uint64_t> checked{0};
            std::atomic<uint64_t> alive{0};
            std::atomic<uint64_t> reconnected{0};
        };

        inline HealthStats& health_stats() { return stats_; }

    private:
        std::string host_;
        std::string port_;
        std::string user_;
        std::string db_;
        std::string password_;

        usub::queue::concurrent::MPMCQueue<std::shared_ptr<PgConnectionLibpq>> idle_;

        size_t max_pool_;
        std::atomic<size_t> live_count_;

        PgPoolHealthConfig health_cfg_;

        HealthStats stats_;

        std::unique_ptr<PgHealthChecker> health_checker_;

        static std::unique_ptr<PgPool> instance_;
    };

    template <typename... Args>
    usub::uvent::task::Awaitable<QueryResult>
    PgPool::query_on(std::shared_ptr<PgConnectionLibpq> const& conn,
                     const std::string& sql,
                     Args&&... args)
    {
        if (!conn || !conn->connected())
        {
            QueryResult bad;
            bad.ok = false;
            bad.code = PgErrorCode::ConnectionClosed;
            bad.error = "connection invalid";
            bad.rows_valid = false;
            co_return bad;
        }

        if constexpr (sizeof...(Args) == 0)
        {
            QueryResult qr = co_await conn->exec_simple_query_nonblocking(sql);
            co_return qr;
        }
        else
        {
            QueryResult qr = co_await conn->exec_param_query_nonblocking(
                sql,
                std::forward<Args>(args)...
            );
            co_return qr;
        }
    }

    template <typename... Args>
    usub::uvent::task::Awaitable<QueryResult>
    PgPool::query_awaitable(const std::string& sql, Args&&... args)
    {
        auto conn = co_await acquire_connection();

        QueryResult qr = co_await query_on(
            conn,
            sql,
            std::forward<Args>(args)...
        );

        release_connection(conn);
        co_return qr;
    }
} // namespace usub::pg

#endif // PGPOOL_H
