#include "upq/PgRouting.h"
#include <algorithm>
#include <chrono>

using namespace std::chrono;

namespace usub::pg
{
    static size_t pool_cap_for(NodeRole role, const PoolLimits& lim)
    {
        return (role == NodeRole::Analytics) ? lim.analytics_max_conns : lim.default_max_conns;
    }

    PgConnector::PgConnector(Config cfg) : cfg_(std::move(cfg))
    {
        this->nodes_.reserve(this->cfg_.nodes.size());
        for (const auto& ep : this->cfg_.nodes)
        {
            auto pool = std::make_unique<PgPool>(
                ep.host, ep.port, ep.user, ep.db, ep.password,
                ep.max_pool ? ep.max_pool : pool_cap_for(ep.role, this->cfg_.limits)
            );
            this->nodes_.push_back(Node{ep, std::move(pool), {}});
        }
        for (const auto& name : this->cfg_.primary_failover)
        {
            auto it = std::find_if(this->nodes_.begin(), this->nodes_.end(),
                                   [&](const Node& n) { return n.ep.name == name; });
            if (it != this->nodes_.end())
                this->primary_failover_idx_.push_back(size_t(std::distance(this->nodes_.begin(), it)));
        }
        if (this->primary_failover_idx_.empty())
        {
            for (size_t i = 0; i < this->nodes_.size(); ++i)
                if (this->nodes_[i].ep.role == NodeRole::Primary)
                    this->primary_failover_idx_.push_back(i);
            for (size_t i = 0; i < this->nodes_.size(); ++i)
                if (this->nodes_[i].ep.role == NodeRole::SyncReplica)
                    this->primary_failover_idx_.push_back(i);
            for (size_t i = 0; i < this->nodes_.size(); ++i)
                if (this->nodes_[i].ep.role == NodeRole::AsyncReplica)
                    this->primary_failover_idx_.push_back(i);
        }
    }

    PgPool* PgConnector::route(const RouteHint& hint)
    {
        if (hint.kind == QueryKind::Write || hint.kind == QueryKind::DDL
            || hint.consistency == Consistency::Strong || hint.read_my_writes)
        {
            auto* p = this->pick_primary();
            return p ? p->pool.get() : nullptr;
        }
        auto* r = this->pick_best_replica(hint);
        if (r) return r->pool.get();
        auto* p = this->pick_primary();
        return p ? p->pool.get() : nullptr;
    }

    PgPool* PgConnector::route_for_tx(const PgTransactionConfig& cfg_tx)
    {
        const auto eff_consistency =
            (cfg_tx.isolation == TxIsolationLevel::Serializable)
                ? Consistency::Strong
                : this->cfg_.routing.default_consistency;

        if (!cfg_tx.read_only || eff_consistency == Consistency::Strong)
        {
            auto* p = this->pick_primary();
            return p ? p->pool.get() : nullptr;
        }

        if (cfg_tx.deferrable)
        {
            Node* best = nullptr;
            for (auto& n : this->nodes_)
            {
                if (n.ep.role != NodeRole::SyncReplica ||
                    !n.stats.healthy ||
                    !this->is_usable(n.ep.role))
                    continue;
                if (!best || n.stats.replay_lag < best->stats.replay_lag)
                    best = &n;
            }
            if (best) return best->pool.get();
            auto* p = this->pick_primary();
            return p ? p->pool.get() : nullptr;
        }

        RouteHint rh{
            .kind = QueryKind::Read,
            .consistency = eff_consistency,
            .staleness = this->cfg_.routing.bounded_staleness,
            .read_my_writes = false
        };
        if (auto* r = this->pick_best_replica(rh)) return r->pool.get();

        auto* p = this->pick_primary();
        return p ? p->pool.get() : nullptr;
    }

    PgConnector::Node* PgConnector::pick_primary()
    {
        for (auto idx : this->primary_failover_idx_)
        {
            auto& n = this->nodes_[idx];
            if (n.ep.role == NodeRole::Primary && n.stats.healthy && n.cb_state != 2 && this->is_usable(n.ep.role))
                return &n;
        }
        for (auto& n : this->nodes_)
            if (n.ep.role == NodeRole::Primary && n.cb_state != 2 && this->is_usable(n.ep.role))
                return &n;
        return nullptr;
    }

    PgConnector::Node* PgConnector::pick_best_replica(const RouteHint& hint)
    {
        Node* best = nullptr;
        auto ok_stale = [&](const Node& n)-> bool
        {
            if (hint.consistency != Consistency::BoundedStaleness) return true;
            if (n.stats.replay_lag > hint.staleness.max_staleness) return false;
            if (hint.staleness.max_lsn_lag && n.stats.lsn_lag > hint.staleness.max_lsn_lag) return false;
            return true;
        };
        auto better = [&](const Node* a, const Node* b)-> bool
        {
            if (a->stats.rtt != b->stats.rtt) return a->stats.rtt < b->stats.rtt;
            return a->ep.weight > b->ep.weight;
        };
        for (auto& n : this->nodes_)
        {
            if (!this->is_replica(n.ep.role) || !n.stats.healthy || n.cb_state == 2 || !this->is_usable(n.ep.role))
                continue;
            if (!ok_stale(n)) continue;
            if (!best || better(&n, best)) best = &n;
        }
        return best;
    }

    void PgConnector::apply_circuit_breaker(Node& n, bool ok)
    {
        auto now = std::chrono::steady_clock::now();
        if (ok)
        {
            if (n.cb_state == 1 && now >= n.cb_until) n.cb_state = 0;
            if (n.cb_state == 2 && now >= n.cb_until) n.cb_state = 1;
            return;
        }
        if (n.cb_state == 0)
        {
            n.cb_state = 2;
            n.cb_until = now + milliseconds(500);
        }
        else if (n.cb_state == 1)
        {
            n.cb_state = 2;
            n.cb_until = now + milliseconds(1000);
        }
        else { n.cb_until = now + milliseconds(1500); }
    }

    usub::uvent::task::Awaitable<bool> PgConnector::probe_healthy(PgPool& pool)
    {
        auto qr = co_await pool.query_awaitable("SELECT 1");
        co_return qr.ok;
    }

    usub::uvent::task::Awaitable<std::chrono::milliseconds>
    PgConnector::probe_rtt(PgPool& pool, const std::string& sql)
    {
        using clock = std::chrono::steady_clock;
        auto t0 = clock::now();
        auto qr = co_await pool.query_awaitable(sql);
        auto t1 = clock::now();
        if (!qr.ok) co_return milliseconds{9999};
        co_return std::chrono::duration_cast<milliseconds>(t1 - t0);
    }

    usub::uvent::task::Awaitable<std::pair<std::chrono::milliseconds, uint64_t>>
    PgConnector::probe_replication_lag(PgPool& pool)
    {
        struct Row
        {
            int64_t lag_ms;
            int64_t lsn_lag;
        };
        auto opt = co_await pool.query_reflect_one<Row>(R"SQL(
        SELECT
          COALESCE( (EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) * 1000)::bigint, 0 ) AS lag_ms,
          COALESCE( pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::bigint, 0 ) AS lsn_lag
    )SQL");
        if (!opt.has_value())
            co_return std::make_pair(milliseconds{0}, uint64_t{0});
        co_return std::make_pair(milliseconds{opt->lag_ms}, static_cast<uint64_t>(opt->lsn_lag));
    }

    usub::uvent::task::Awaitable<void> PgConnector::health_tick()
    {
        for (auto& n : this->nodes_)
        {
            if (!this->is_usable(n.ep.role)) continue;

            bool ok = co_await this->probe_healthy(*n.pool);
            auto rtt = co_await this->probe_rtt(*n.pool, this->cfg_.health.rtt_probe_sql);
            auto [lag_ms, lsn_lag] = co_await this->probe_replication_lag(*n.pool);

            n.stats.healthy = ok;
            n.stats.rtt = rtt;
            n.stats.replay_lag = lag_ms;
            n.stats.lsn_lag = lsn_lag;

            if (n.ep.role == NodeRole::Primary && n.stats.replay_lag.count() > 0)
                n.stats.healthy = false;

            this->apply_circuit_breaker(n, n.stats.healthy);
        }
        co_return;
    }

    PgPool* PgConnector::pin(const std::string& node_name, const RouteHint&)
    {
        auto it = std::find_if(this->nodes_.begin(), this->nodes_.end(),
                               [&](const Node& n) { return n.ep.name == node_name; });
        if (it == this->nodes_.end() || !it->stats.healthy || it->cb_state == 2 || !this->is_usable(it->ep.role))
            return nullptr;
        return it->pool.get();
    }
}
