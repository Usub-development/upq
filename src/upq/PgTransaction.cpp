#include "upq/PgTransaction.h"

namespace usub::pg
{
    PgTransaction::PgTransaction(PgPool* pool)
        : pool_(pool)
    {
    }

    PgTransaction::~PgTransaction()
    {
        if (this->active_)
        {
        }
    }

    usub::uvent::task::Awaitable<bool> PgTransaction::begin()
    {
        if (this->active_)
        {
            co_return true;
        }

        this->conn_ = co_await this->pool_->acquire_connection();

        if (!this->conn_ || !this->conn_->connected())
        {
            this->conn_.reset();
            co_return false;
        }

        {
            QueryResult r_begin = co_await this->pool_->query_on(this->conn_, "BEGIN");
            if (!r_begin.ok)
            {
                this->pool_->release_connection(this->conn_);
                this->conn_.reset();
                this->active_ = false;
                this->committed_ = false;
                this->rolled_back_ = false;
                co_return false;
            }
        }

        this->active_ = true;
        this->committed_ = false;
        this->rolled_back_ = false;
        co_return true;
    }

    usub::uvent::task::Awaitable<bool> PgTransaction::commit()
    {
        if (!this->active_)
        {
            co_return false;
        }

        if (!this->conn_ || !this->conn_->connected())
        {
            this->committed_ = false;
            this->rolled_back_ = true;
            this->active_ = false;

            if (this->conn_)
            {
                this->pool_->release_connection(this->conn_);
                this->conn_.reset();
            }

            co_return false;
        }

        QueryResult r_commit = co_await this->pool_->query_on(this->conn_, "COMMIT");
        if (!r_commit.ok)
        {
            this->committed_ = false;
            this->rolled_back_ = true;
            this->active_ = false;

            this->pool_->release_connection(this->conn_);
            this->conn_.reset();

            co_return false;
        }

        this->committed_ = true;
        this->rolled_back_ = false;
        this->active_ = false;

        this->pool_->release_connection(this->conn_);
        this->conn_.reset();

        co_return true;
    }

    usub::uvent::task::Awaitable<void> PgTransaction::rollback()
    {
        if (!this->active_)
        {
            co_return;
        }

        if (this->conn_ && this->conn_->connected())
        {
            QueryResult r_rb = co_await this->pool_->query_on(this->conn_, "ROLLBACK");
            (void)r_rb;
        }

        this->committed_ = false;
        this->rolled_back_ = true;
        this->active_ = false;

        if (this->conn_)
        {
            this->pool_->release_connection(this->conn_);
            this->conn_.reset();
        }

        co_return;
    }

    usub::uvent::task::Awaitable<void> PgTransaction::finish()
    {
        if (!this->active_)
        {
            if (this->conn_)
            {
                this->pool_->release_connection(this->conn_);
                this->conn_.reset();
            }
            co_return;
        }

        co_await rollback();
        co_return;
    }

    usub::uvent::task::Awaitable<void> PgTransaction::abort()
    {
        if (!this->active_)
        {
            co_return;
        }

        if (this->conn_ && this->conn_->connected())
        {
            QueryResult r_rb = co_await this->pool_->query_on(this->conn_, "ABORT");
            (void)r_rb;
        }

        this->committed_ = false;
        this->rolled_back_ = true;
        this->active_ = false;

        if (this->conn_)
        {
            this->pool_->release_connection(this->conn_);
            this->conn_.reset();
        }

        co_return;
    }
} // namespace usub::pg
