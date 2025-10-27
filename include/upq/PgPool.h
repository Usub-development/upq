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
    class PgPool
    {
    public:
        PgPool(std::string host,
               std::string port,
               std::string user,
               std::string db,
               std::string password,
               size_t max_pool_size = 32,
               size_t queue_capacity = 64);

        static void init_global(const std::string& host,
                                const std::string& port,
                                const std::string& user,
                                const std::string& db,
                                const std::string& password,
                                size_t max_pool_size = 32,
                                size_t queue_capacity = 64);

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

    private:
        std::string host_;
        std::string port_;
        std::string user_;
        std::string db_;
        std::string password_;

        usub::queue::concurrent::MPMCQueue<std::shared_ptr<PgConnectionLibpq>> idle_;

        size_t max_pool_;
        std::atomic<size_t> live_count_;

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
            bad.error = "connection invalid";
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
