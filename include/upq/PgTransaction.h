//
// Created by root on 10/27/25.
//

#ifndef PGTRANSACTION_H
#define PGTRANSACTION_H

#include <memory>
#include <string>

#include "PgPool.h"
#include "PgConnection.h"
#include "PgTypes.h"
#include "uvent/Uvent.h"

namespace usub::pg
{
    class PgTransaction
    {
    public:
        explicit PgTransaction(PgPool* pool = &PgPool::instance());
        ~PgTransaction();

        usub::uvent::task::Awaitable<bool> begin();

        template <typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query(const std::string& sql, Args&&... args);

        usub::uvent::task::Awaitable<bool> commit();

        usub::uvent::task::Awaitable<void> rollback();

        usub::uvent::task::Awaitable<void> finish();

        bool is_active() const noexcept { return active_; }
        bool is_committed() const noexcept { return committed_; }
        bool is_rolled_back() const noexcept { return rolled_back_; }

    private:
        PgPool* pool_{nullptr};
        std::shared_ptr<PgConnectionLibpq> conn_;

        bool active_{false};
        bool committed_{false};
        bool rolled_back_{false};
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

            this->pool_->release_connection(this->conn_);
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

#endif //PGTRANSACTION_H