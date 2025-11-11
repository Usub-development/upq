#ifndef PGPOOL_H
#define PGPOOL_H

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <atomic>
#include <chrono>
#include <expected>

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
               size_t max_pool_size = 32);

        ~PgPool();

        usub::uvent::task::Awaitable<std::shared_ptr<PgConnectionLibpq>>
        acquire_connection();

        void release_connection(std::shared_ptr<PgConnectionLibpq> conn);

        usub::uvent::task::Awaitable<void>
        release_connection_async(std::shared_ptr<PgConnectionLibpq> conn);

        template <typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query_on(std::shared_ptr<PgConnectionLibpq> const& conn,
                 const std::string& sql,
                 Args&&... args);

        template <typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query_awaitable(const std::string& sql, Args&&... args);

        template <class T>
        usub::uvent::task::Awaitable<std::vector<T>>
        query_on_reflect(std::shared_ptr<PgConnectionLibpq> const& conn,
                         const std::string& sql);

        template <class T>
        usub::uvent::task::Awaitable<std::optional<T>>
        query_on_reflect_one(std::shared_ptr<PgConnectionLibpq> const& conn,
                             const std::string& sql);

        template <class T>
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T>>
        query_reflect(const std::string& sql);

        template <class T>
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T>>
        query_reflect_one(const std::string& sql);

        template <class T>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError>>
        query_on_reflect_expected(std::shared_ptr<PgConnectionLibpq> const& conn,
                                  const std::string& sql)
        {
            QueryResult qr = co_await query_on(conn, sql);
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, std::move(qr.error), std::move(qr.err_detail)});
            auto rows = co_await conn->exec_simple_query_nonblocking<T>(sql);
            co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(rows)};
        }

        template <class T>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError>>
        query_on_reflect_expected_one(std::shared_ptr<PgConnectionLibpq> const& conn,
                                      const std::string& sql)
        {
            QueryResult qr = co_await query_on(conn, sql);
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, std::move(qr.error), std::move(qr.err_detail)});

            auto row = co_await conn->exec_simple_query_one_nonblocking<T>(sql);
            if (!row)
                co_return std::unexpected(PgOpError{PgErrorCode::Unknown, "no rows", {}});
            co_return std::expected<T, PgOpError>{std::in_place, std::move(*row)};
        }

        template <class T>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError>>
        query_reflect_expected(const std::string& sql)
        {
            auto conn = co_await acquire_connection();
            auto res = co_await query_on_reflect_expected<T>(conn, sql);
            co_await release_connection_async(conn);
            co_return res;
        }

        template <class T>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError>>
        query_reflect_expected_one(const std::string& sql)
        {
            auto conn = co_await acquire_connection();
            auto res = co_await query_on_reflect_expected_one<T>(conn, sql);
            co_await release_connection_async(conn);
            co_return res;
        }

        template <class T, typename... Args>
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T>>
        query_on_reflect(std::shared_ptr<PgConnectionLibpq> const& conn,
                         const std::string& sql, Args&&... args);

        template <class T, typename... Args>
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T>>
        query_on_reflect_one(std::shared_ptr<PgConnectionLibpq> const& conn,
                             const std::string& sql, Args&&... args);

        template <class T, typename... Args>
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T>>
        query_reflect(const std::string& sql, Args&&... args);

        template <class T, typename... Args>
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T>>
        query_reflect_one(const std::string& sql, Args&&... args);

        template <class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError>>
        query_on_reflect_expected(std::shared_ptr<PgConnectionLibpq> const& conn,
                                  const std::string& sql, Args&&... args)
        {
            QueryResult qr = co_await query_on(conn, sql, std::forward<Args>(args)...);
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, std::move(qr.error), std::move(qr.err_detail)});

            auto rows = co_await conn->exec_param_query_nonblocking<T>(
                sql, std::forward<Args>(args)...);
            co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(rows)};
        }

        template <class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError>>
        query_on_reflect_expected_one(std::shared_ptr<PgConnectionLibpq> const& conn,
                                      const std::string& sql, Args&&... args)
        {
            QueryResult qr = co_await query_on(conn, sql, std::forward<Args>(args)...);
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, std::move(qr.error), std::move(qr.err_detail)});

            auto row = co_await conn->exec_param_query_one_nonblocking<T>(
                sql, std::forward<Args>(args)...);
            if (!row)
                co_return std::unexpected(PgOpError{PgErrorCode::Unknown, "no rows", {}});
            co_return std::expected<T, PgOpError>{std::in_place, std::move(*row)};
        }

        template <class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError>>
        query_reflect_expected(const std::string& sql, Args&&... args)
        {
            auto conn = co_await acquire_connection();
            auto res = co_await query_on_reflect_expected<T>(conn, sql, std::forward<Args>(args)...);
            co_await release_connection_async(conn);
            co_return res;
        }

        template <class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError>>
        query_reflect_expected_one(const std::string& sql, Args&&... args)
        {
            auto conn = co_await acquire_connection();
            auto res = co_await query_on_reflect_expected_one<T>(conn, sql, std::forward<Args>(args)...);
            co_await release_connection_async(conn);
            co_return res;
        }

        template <class Obj>
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect_on(std::shared_ptr<PgConnectionLibpq> const& conn,
                        const std::string& sql,
                        const Obj& obj);

        template <class Obj>
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect(const std::string& sql, const Obj& obj);

        inline std::string host() { return this->host_; }
        inline std::string port() { return this->port_; }
        inline std::string user() { return this->user_; }
        inline std::string db() { return this->db_; }
        inline std::string password() { return this->password_; }

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

        HealthStats stats_;
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

        co_await release_connection_async(conn);
        co_return qr;
    }

    template <class T>
    usub::uvent::task::Awaitable<std::vector<T>>
    PgPool::query_on_reflect(std::shared_ptr<PgConnectionLibpq> const& conn,
                             const std::string& sql)
    {
        if (!conn || !conn->connected())
            co_return std::vector<T>{};

        auto rows = co_await conn->exec_simple_query_nonblocking<T>(sql);
        co_return rows;
    }

    template <class T>
    usub::uvent::task::Awaitable<std::optional<T>>
    PgPool::query_on_reflect_one(std::shared_ptr<PgConnectionLibpq> const& conn,
                                 const std::string& sql)
    {
        if (!conn || !conn->connected())
            co_return std::nullopt;

        auto row = co_await conn->exec_simple_query_one_nonblocking<T>(sql);
        co_return row;
    }

    template <class T>
    usub::uvent::task::Awaitable<std::vector<T>>
    PgPool::query_reflect(const std::string& sql)
    {
        auto conn = co_await acquire_connection();
        auto rows = co_await query_on_reflect<T>(conn, sql);
        co_await release_connection_async(conn);
        co_return rows;
    }

    template <class T>
    usub::uvent::task::Awaitable<std::optional<T>>
    PgPool::query_reflect_one(const std::string& sql)
    {
        auto conn = co_await acquire_connection();
        auto row = co_await query_on_reflect_one<T>(conn, sql);
        co_await release_connection_async(conn);
        co_return row;
    }

    template <class T, typename... Args>
    usub::uvent::task::Awaitable<std::vector<T>>
    PgPool::query_on_reflect(std::shared_ptr<PgConnectionLibpq> const& conn,
                             const std::string& sql, Args&&... args)
    {
        if (!conn || !conn->connected())
            co_return std::vector<T>{};

        auto rows = co_await conn->exec_param_query_nonblocking<T>(
            sql, std::forward<Args>(args)...);
        co_return rows;
    }

    template <class T, typename... Args>
    usub::uvent::task::Awaitable<std::optional<T>>
    PgPool::query_on_reflect_one(std::shared_ptr<PgConnectionLibpq> const& conn,
                                 const std::string& sql, Args&&... args)
    {
        if (!conn || !conn->connected())
            co_return std::nullopt;

        auto row = co_await conn->exec_param_query_one_nonblocking<T>(
            sql, std::forward<Args>(args)...);
        co_return row;
    }

    template <class T, typename... Args>
    usub::uvent::task::Awaitable<std::vector<T>>
    PgPool::query_reflect(const std::string& sql, Args&&... args)
    {
        auto conn = co_await acquire_connection();
        auto rows = co_await query_on_reflect<T>(
            conn, sql, std::forward<Args>(args)...);
        co_await release_connection_async(conn);
        co_return rows;
    }

    template <class T, typename... Args>
    usub::uvent::task::Awaitable<std::optional<T>>
    PgPool::query_reflect_one(const std::string& sql, Args&&... args)
    {
        auto conn = co_await acquire_connection();
        auto row = co_await query_on_reflect_one<T>(
            conn, sql, std::forward<Args>(args)...);
        co_await release_connection_async(conn);
        co_return row;
    }

    template <class Obj>
    usub::uvent::task::Awaitable<QueryResult>
    PgPool::exec_reflect_on(std::shared_ptr<PgConnectionLibpq> const& conn,
                            const std::string& sql,
                            const Obj& obj)
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

        QueryResult qr = co_await conn->exec_param_query_nonblocking(sql, obj);
        co_return qr;
    }

    template <class Obj>
    usub::uvent::task::Awaitable<QueryResult>
    PgPool::exec_reflect(const std::string& sql, const Obj& obj)
    {
        auto conn = co_await acquire_connection();
        auto qr = co_await exec_reflect_on(conn, sql, obj);
        co_await release_connection_async(conn);
        co_return qr;
    }
} // namespace usub::pg

#endif
