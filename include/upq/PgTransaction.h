#ifndef PGTRANSACTION_H
#define PGTRANSACTION_H

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <cstdint>
#include <atomic>
#include <optional>
#include <vector>
#include <expected>

#include "PgPool.h"
#include "PgConnection.h"
#include "PgTypes.h"
#include "PgReflect.h"
#include "uvent/Uvent.h"

namespace usub::pg
{
    enum class TxIsolationLevel : uint8_t
    {
        Default = 0,
        ReadCommitted,
        RepeatableRead,
        Serializable
    };

    struct PgTransactionConfig
    {
        TxIsolationLevel isolation = TxIsolationLevel::Default;
        bool read_only = false;
        bool deferrable = false;
    };

    class PgTransaction
    {
    public:
        explicit PgTransaction(PgPool* pool, PgTransactionConfig cfg = {});
        ~PgTransaction();

        usub::uvent::task::Awaitable<bool> begin();

        usub::uvent::task::Awaitable<std::optional<PgOpError>>
        begin_errored();

        template <typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query(const std::string& sql, Args&&... args);

        template <class Obj>
        usub::uvent::task::Awaitable<QueryResult>
        query_reflect(const std::string& sql, const Obj& obj)
        {
            if (!active_ || !conn_ || !conn_->connected())
            {
                QueryResult bad;
                bad.ok = false;
                bad.code = PgErrorCode::InvalidFuture;
                bad.error = "transaction not active";
                bad.rows_valid = false;
                co_return bad;
            }
            co_return co_await conn_->exec_param_query_nonblocking(sql, obj);
        }

        template <class Obj>
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect(const std::string& sql, const Obj& obj)
        {
            co_return co_await query_reflect(sql, obj);
        }

        template <class T>
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T>>
        query_reflect(const std::string& sql)
        {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::vector<T>{};
            co_return co_await conn_->exec_simple_query_nonblocking<T>(sql);
        }

        template <class T>
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T>>
        query_reflect_one(const std::string& sql)
        {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::optional<T>{};
            co_return co_await conn_->exec_simple_query_one_nonblocking<T>(sql);
        }

        template <class T>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError>>
        query_reflect_expected(const std::string& sql)
        {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}}
                );

            QueryResult qr = co_await conn_->exec_simple_query_nonblocking(sql);
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});

            try
            {
                auto vec = usub::pg::map_all_reflect_named<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            }
            catch (...)
            {
                auto vec = usub::pg::map_all_reflect_positional<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            }
        }

        template <class T>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError>>
        query_reflect_expected_one(const std::string& sql)
        {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}}
                );

            QueryResult qr = co_await conn_->exec_simple_query_nonblocking(sql);
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});
            if (qr.rows.empty())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::Unknown, "no rows", {}}
                );

            try
            {
                auto v = usub::pg::map_single_reflect_named<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            }
            catch (...)
            {
                auto v = usub::pg::map_single_reflect_positional<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            }
        }

        template <class T, class Obj>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError>>
        query_reflect_expected(const std::string& sql, const Obj& obj)
        {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}}
                );

            QueryResult qr = co_await conn_->exec_param_query_nonblocking(sql, obj);
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});

            try
            {
                auto vec = usub::pg::map_all_reflect_named<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            }
            catch (...)
            {
                auto vec = usub::pg::map_all_reflect_positional<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            }
        }

        template <class T, class Obj>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError>>
        query_reflect_expected_one(const std::string& sql, const Obj& obj)
        {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}}
                );

            QueryResult qr = co_await conn_->exec_param_query_nonblocking(sql, obj);
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});
            if (qr.rows.empty())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::Unknown, "no rows", {}}
                );

            try
            {
                auto v = usub::pg::map_single_reflect_named<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            }
            catch (...)
            {
                auto v = usub::pg::map_single_reflect_positional<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            }
        }

        template <class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError>>
        query_reflect_expected(const std::string& sql, Args&&... args)
        {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}}
                );

            QueryResult qr = co_await conn_->exec_param_query_nonblocking(
                sql, std::forward<Args>(args)...);
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});

            try
            {
                auto vec = usub::pg::map_all_reflect_named<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            }
            catch (...)
            {
                auto vec = usub::pg::map_all_reflect_positional<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            }
        }

        template <class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError>>
        query_reflect_expected_one(const std::string& sql, Args&&... args)
        {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}}
                );

            QueryResult qr = co_await conn_->exec_param_query_nonblocking(
                sql, std::forward<Args>(args)...);
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});
            if (qr.rows.empty())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::Unknown, "no rows", {}}
                );

            try
            {
                auto v = usub::pg::map_single_reflect_named<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            }
            catch (...)
            {
                auto v = usub::pg::map_single_reflect_positional<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            }
        }

        usub::uvent::task::Awaitable<bool> commit();
        usub::uvent::task::Awaitable<void> rollback();
        usub::uvent::task::Awaitable<void> finish();
        usub::uvent::task::Awaitable<void> abort();

        bool is_active() const noexcept { return active_; }
        bool is_committed() const noexcept { return committed_; }
        bool is_rolled_back() const noexcept { return rolled_back_; }

        class PgSubtransaction
        {
        public:
            PgSubtransaction(PgTransaction& parent, std::string savepoint_name);
            ~PgSubtransaction();

            usub::uvent::task::Awaitable<bool> begin();
            usub::uvent::task::Awaitable<bool> commit();
            usub::uvent::task::Awaitable<void> rollback();

            bool is_active() const noexcept { return active_; }
            bool is_committed() const noexcept { return committed_; }
            bool is_rolled_back() const noexcept { return rolled_back_; }

            template <typename... Args>
            usub::uvent::task::Awaitable<QueryResult>
            query(const std::string& sql, Args&&... args)
            {
                co_return co_await parent_.query(sql, std::forward<Args>(args)...);
            }

            template <class Obj>
            usub::uvent::task::Awaitable<QueryResult>
            query_reflect(const std::string& sql, const Obj& obj)
            {
                co_return co_await parent_.query_reflect(sql, obj);
            }

            template <class Obj>
            usub::uvent::task::Awaitable<QueryResult>
            exec_reflect(const std::string& sql, const Obj& obj)
            {
                co_return co_await parent_.exec_reflect(sql, obj);
            }

            template <class T, typename... Args>
            usub::uvent::task::Awaitable<std::expected<T, PgOpError>>
            query_reflect_expected_one(const std::string& sql, Args&&... args)
            {
                co_return co_await parent_.template query_reflect_expected_one<T>(
                    sql, std::forward<Args>(args)...);
            }

        private:
            PgTransaction& parent_;
            std::string sp_name_;
            bool active_{false};
            bool committed_{false};
            bool rolled_back_{false};
        };

        PgSubtransaction make_subtx();
        std::shared_ptr<PgConnectionLibpq> connection() const { return conn_; }

    private:
        PgPool* pool_;
        PgTransactionConfig cfg_;
        std::shared_ptr<PgConnectionLibpq> conn_;

        bool active_{false};
        bool committed_{false};
        bool rolled_back_{false};

        bool emulate_readonly_autocommit_{false};

        usub::uvent::task::Awaitable<bool> send_sql_nocheck(const std::string& sql);
        static std::string build_begin_sql(const PgTransactionConfig& cfg);
    };

    template <typename... Args>
    usub::uvent::task::Awaitable<QueryResult>
    PgTransaction::query(const std::string& sql, Args&&... args)
    {
        if (!active_ || !conn_)
        {
            QueryResult bad;
            bad.ok = false;
            bad.code = PgErrorCode::InvalidFuture;
            bad.error = "transaction not active";
            bad.rows_valid = false;
            co_return bad;
        }

        if (!conn_->connected())
        {
            QueryResult bad;
            bad.ok = false;
            bad.code = PgErrorCode::ConnectionClosed;
            bad.error = "connection lost in transaction";
            bad.rows_valid = false;
            active_ = false;
            rolled_back_ = true;
            committed_ = false;

            pool_->mark_dead(conn_);
            conn_.reset();
            co_return bad;
        }

        QueryResult qr = co_await pool_->query_on(conn_, sql, std::forward<Args>(args)...);
        if (is_fatal_connection_error(qr))
        {
            pool_->mark_dead(conn_);
            conn_.reset();
            active_ = false;
            rolled_back_ = true;
            committed_ = false;
        }
        co_return qr;
    }
} // namespace usub::pg

#endif // PGTRANSACTION_H
