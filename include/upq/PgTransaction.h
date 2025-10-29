#ifndef PGTRANSACTION_H
#define PGTRANSACTION_H

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <cstdint>
#include <atomic>

#include "PgPool.h"
#include "PgConnection.h"
#include "PgTypes.h"
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
        explicit PgTransaction(
            PgPool* pool = &PgPool::instance(),
            PgTransactionConfig cfg = {}
        );

        ~PgTransaction();

        usub::uvent::task::Awaitable<bool> begin();

        template <typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query(const std::string& sql, Args&&... args);

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
            PgSubtransaction(PgTransaction& parent,
                             std::string savepoint_name);

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
        PgPool* pool_{nullptr};
        PgTransactionConfig cfg_;
        std::shared_ptr<PgConnectionLibpq> conn_;

        bool active_{false};
        bool committed_{false};
        bool rolled_back_{false};

        usub::uvent::task::Awaitable<bool> send_sql_nocheck(const std::string& sql);
        static std::string build_begin_sql(const PgTransactionConfig& cfg);
    };

    template <typename... Args>
    usub::uvent::task::Awaitable<QueryResult>
    PgTransaction::query(const std::string& sql, Args&&... args)
    {
        if (!this->active_ || !this->conn_)
        {
            QueryResult bad;
            bad.ok = false;
            bad.code = PgErrorCode::InvalidFuture;
            bad.error = "transaction not active";
            bad.rows_valid = false;
            co_return bad;
        }

        if (!this->conn_->connected())
        {
            QueryResult bad;
            bad.ok = false;
            bad.code = PgErrorCode::ConnectionClosed;
            bad.error = "connection lost in transaction";
            bad.rows_valid = false;

            this->active_ = false;
            this->rolled_back_ = true;
            this->committed_ = false;

            co_await this->pool_->release_connection_async(this->conn_);
            this->conn_.reset();

            co_return bad;
        }

        QueryResult qr = co_await this->pool_->query_on(
            this->conn_,
            sql,
            std::forward<Args>(args)...
        );

        co_return qr;
    }

} // namespace usub::pg

#endif // PGTRANSACTION_H
