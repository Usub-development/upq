#include "upq/PgTransaction.h"
#include <atomic>

namespace usub::pg
{
    std::string PgTransaction::build_begin_sql(const PgTransactionConfig& cfg)
    {
        bool any_opts =
            (cfg.isolation != TxIsolationLevel::Default) ||
            (cfg.read_only) ||
            (cfg.deferrable) ||
            (!cfg.read_only);

        if (!any_opts)
        {
            return "BEGIN";
        }

        std::string out = "BEGIN";

        switch (cfg.isolation)
        {
        case TxIsolationLevel::ReadCommitted:
            out += " ISOLATION LEVEL READ COMMITTED";
            break;
        case TxIsolationLevel::RepeatableRead:
            out += " ISOLATION LEVEL REPEATABLE READ";
            break;
        case TxIsolationLevel::Serializable:
            out += " ISOLATION LEVEL SERIALIZABLE";
            break;
        case TxIsolationLevel::Default:
        default:
            break;
        }

        if (cfg.read_only)
        {
            out += " READ ONLY";
        }
        else
        {
            out += " READ WRITE";
        }

        if (cfg.deferrable)
        {
            out += " DEFERRABLE";
        }

        return out;
    }

    PgTransaction::PgTransaction(PgPool* pool, PgTransactionConfig cfg)
        : pool_(pool)
          , cfg_(cfg)
    {
    }

    PgTransaction::~PgTransaction()
    {
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
            const std::string bsql = build_begin_sql(this->cfg_);
            QueryResult r_begin = co_await this->pool_->query_on(this->conn_, bsql);
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

    usub::uvent::task::Awaitable<bool> PgTransaction::send_sql_nocheck(const std::string& sql)
    {
        if (!this->active_ || !this->conn_ || !this->conn_->connected())
        {
            co_return false;
        }
        QueryResult r = co_await this->pool_->query_on(this->conn_, sql);
        co_return r.ok;
    }

    static std::atomic<uint64_t> g_subtx_id{0};

    PgTransaction::PgSubtransaction PgTransaction::make_subtx()
    {
        uint64_t id = g_subtx_id.fetch_add(1, std::memory_order_relaxed);
        std::string name = "uv_sp_" + std::to_string(id);
        return PgSubtransaction(*this, std::move(name));
    }

    PgTransaction::PgSubtransaction::PgSubtransaction(
        PgTransaction& parent,
        std::string savepoint_name)
        : parent_(parent)
          , sp_name_(std::move(savepoint_name))
    {
    }

    PgTransaction::PgSubtransaction::~PgSubtransaction()
    {
    }

    usub::uvent::task::Awaitable<bool>
    PgTransaction::PgSubtransaction::begin()
    {
        if (!parent_.active_ || !parent_.conn_ || !parent_.conn_->connected())
        {
            co_return false;
        }

        std::string cmd = "SAVEPOINT " + this->sp_name_;
        QueryResult r = co_await parent_.pool_->query_on(parent_.conn_, cmd);
        if (!r.ok)
        {
            co_return false;
        }

        this->active_ = true;
        this->committed_ = false;
        this->rolled_back_ = false;
        co_return true;
    }

    usub::uvent::task::Awaitable<bool>
    PgTransaction::PgSubtransaction::commit()
    {
        if (!this->active_)
        {
            co_return false;
        }

        if (!parent_.conn_ || !parent_.conn_->connected())
        {
            this->active_ = false;
            this->committed_ = false;
            this->rolled_back_ = true;
            co_return false;
        }

        std::string cmd = "RELEASE SAVEPOINT " + this->sp_name_;
        QueryResult r = co_await parent_.pool_->query_on(parent_.conn_, cmd);
        if (!r.ok)
        {
            this->active_ = false;
            this->committed_ = false;
            this->rolled_back_ = true;
            co_return false;
        }

        this->active_ = false;
        this->committed_ = true;
        this->rolled_back_ = false;
        co_return true;
    }

    usub::uvent::task::Awaitable<void>
    PgTransaction::PgSubtransaction::rollback()
    {
        if (!this->active_)
        {
            co_return;
        }

        if (parent_.conn_ && parent_.conn_->connected())
        {
            std::string cmd = "ROLLBACK TO SAVEPOINT " + this->sp_name_;
            QueryResult r = co_await parent_.pool_->query_on(parent_.conn_, cmd);
            (void)r;
        }

        this->active_ = false;
        this->committed_ = false;
        this->rolled_back_ = true;
        co_return;
    }
} // namespace usub::pg
