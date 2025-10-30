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
        using namespace std::chrono;
        auto next_sleep = milliseconds(cfg_.interval_ms ? cfg_.interval_ms : 60000);

        for (;;)
        {
            try
            {
                PgPoolHealthConfig cur = cfg_;

                if (!cur.enabled)
                {
                    co_await usub::uvent::system::this_coroutine::sleep_for(milliseconds(1000));
                    continue;
                }

                this->stats_.iterations.fetch_add(1, std::memory_order_relaxed);

                auto res = co_await pool_.query_awaitable("SELECT 1;");

                if (res.ok)
                {
                    this->stats_.ok_checks.fetch_add(1, std::memory_order_relaxed);
                    next_sleep = milliseconds(cur.interval_ms ? cur.interval_ms : 1000);
                }
                else
                {
                    this->stats_.failed_checks.fetch_add(1, std::memory_order_relaxed);
                    auto backoff = std::min<uint64_t>(cur.interval_ms ? cur.interval_ms * 2 : 2000, 15000);
                    next_sleep = milliseconds(backoff);
                }
            }
            catch (...)
            {
                this->stats_.failed_checks.fetch_add(1, std::memory_order_relaxed);
                next_sleep = milliseconds(std::min<uint64_t>(cfg_.interval_ms ? cfg_.interval_ms * 2 : 2000, 15000));
            }

            co_await usub::uvent::system::this_coroutine::sleep_for(next_sleep);
        }

        co_return;
    }
} // namespace usub::pg
