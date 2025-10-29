#ifndef PGHEALTHCHECKER_H
#define PGHEALTHCHECKER_H

#include <cstdint>
#include <atomic>
#include <memory>
#include <chrono>

#include "uvent/Uvent.h"
#include "upq/PgPool.h"
#include "upq/PgConnection.h"
#include "upq/PgTypes.h"

namespace usub::pg
{
    struct PgHealthStats
    {
        std::atomic<uint64_t> iterations{0};
        std::atomic<uint64_t> ok_checks{0};
        std::atomic<uint64_t> failed_checks{0};
    };

    class PgHealthChecker
    {
    public:
        PgHealthChecker(PgPool& pool, PgPoolHealthConfig cfg = {});

        PgPoolHealthConfig& config();
        const PgPoolHealthConfig& config() const;

        PgHealthStats& stats();
        const PgHealthStats& stats() const;

        usub::uvent::task::Awaitable<void> run();

    private:
        PgPool& pool_;
        PgPoolHealthConfig cfg_;
        PgHealthStats stats_;
    };
} // namespace usub::pg

#endif // PGHEALTHCHECKER_H