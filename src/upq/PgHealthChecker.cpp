#include "upq/PgHealthChecker.h"
#include "uvent/system/SystemContext.h"

namespace usub::pg
{
    PgHealthChecker::PgHealthChecker(PgPool& pool, PgPoolHealthConfig cfg)
        : pool_(pool), cfg_(std::move(cfg))
    {
    }

    PgPoolHealthConfig& PgHealthChecker::config() { return this->cfg_; }
    const PgPoolHealthConfig& PgHealthChecker::config() const { return this->cfg_; }

    PgHealthStats& PgHealthChecker::stats() { return this->stats_; }
    const PgHealthStats& PgHealthChecker::stats() const { return this->stats_; }

    usub::uvent::task::Awaitable<void> PgHealthChecker::run()
    {
        for (;;)
        {
            PgPoolHealthConfig cur_cfg = this->cfg_;

            if (!cur_cfg.enabled)
            {
                co_await usub::uvent::system::this_coroutine::sleep_for(
                    std::chrono::milliseconds(1000)
                );
                continue;
            }

            uint64_t interval = cur_cfg.interval_ms;
            if (interval == 0)
                interval = 1000;

            this->stats_.iterations.fetch_add(1, std::memory_order_relaxed);

            auto conn = co_await this->pool_.acquire_connection();

            bool alive = false;
            if (conn && conn->connected())
            {
                QueryResult ping = co_await conn->exec_simple_query_nonblocking("SELECT 1;");
                if (ping.ok)
                {
                    alive = true;
                    this->stats_.ok_checks.fetch_add(1, std::memory_order_relaxed);
                }
            }

            if (!alive)
            {
                this->stats_.failed_checks.fetch_add(1, std::memory_order_relaxed);
            }

            this->pool_.release_connection(conn);

            co_await usub::uvent::system::this_coroutine::sleep_for(
                std::chrono::milliseconds(interval)
            );
        }

        co_return;
    }
} // namespace usub::pg
