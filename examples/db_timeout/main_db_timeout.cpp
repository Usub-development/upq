// app/src/main.cpp

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <execinfo.h>
#include <iostream>
#include <string>

#include <server/server.h>
#include <upq/PgRouting.h>
#include <upq/PgRoutingBuilder.h>
#include <uvent/Uvent.h>
#include <uvent/system/SystemContext.h>
#include <ulog/ulog.h>

static inline void rtrim_inplace(std::string &s) {
    while (!s.empty() && (s.back() == '\n' || s.back() == '\r'))
        s.pop_back();
}


template<class Name, class Def = std::string_view>
    requires (std::constructible_from<std::string_view, const Name &> &&
              std::constructible_from<std::string_view, const Def &>)
std::string get_docker_secret(Name &&name, Def &&def = {}) {
    std::string_view n{name};
    std::string_view d{def};

    std::string path = std::string("/run/secrets/") + std::string(n);
    std::ifstream file(path);
    if (file) {
        std::string value;
        std::getline(file, value);
        rtrim_inplace(value);
        if (!value.empty())
            return value;
    }

    std::string name_copy{n};
    if (const char *env_val = std::getenv(name_copy.c_str()))
        return env_val;

    return std::string(d);
}

void crashHandler(int sig) {
    void *array[64];
    int size = backtrace(array, 64);
    std::cerr << "!!!!!!!!!!!!=== C++ crash signal: " << sig << " ===!!!!!!!!!!!!\n";
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    _exit(1);
}

void installCrashHandler() {
    signal(SIGSEGV, crashHandler);
    signal(SIGABRT, crashHandler);
    signal(SIGFPE, crashHandler);
    signal(SIGILL, crashHandler);
    signal(SIGBUS, crashHandler);
}

#include "source_dir.h"
constexpr const char *kSrcDir = SOURCE_DIR;

int main() {
    installCrashHandler();

    usub::ulog::ULogInit cfg{
        .trace_path = nullptr,
        .debug_path = nullptr,
        .info_path = nullptr,
        .warn_path = nullptr,
        .error_path = nullptr,
        .critical_path = nullptr,
        .fatal_path = nullptr,
        .flush_interval_ns = 5'000'000'000ULL,
        .queue_capacity = 1024,
        .batch_size = 512,
        .enable_color_stdout = true,
        .json_mode = false,
        .track_metrics = false
    };
    usub::ulog::init(cfg);

    auto config_path = get_docker_secret("SERVER_CONFIG_PATH",
                                         std::string(kSrcDir) + "/examples/db_timeout/config.toml");
    usub::server::Server server(config_path);

    auto host = get_docker_secret("POSTGRESQL_HOST", "localhost");
    auto port = get_docker_secret("POSTGRESQL_PORT", "5433");
    auto user = get_docker_secret("POSTGRESQL_USER", "dev");
    auto db = get_docker_secret("POSTGRESQL_DB", "devdb");
    auto pass = get_docker_secret("POSTGRESQL_PASSWORD", "devpass");

    usub::pg::PgConnector pg =
            usub::pg::PgConnectorBuilder{}
            .node("p1", host, port, user, db, pass, usub::pg::NodeRole::Primary, 1, 64)
            .primary_failover({"p1"})
            .default_consistency(usub::pg::Consistency::Eventual)
            .bounded_staleness(std::chrono::milliseconds{150}, 0)
            .read_my_writes_ttl(std::chrono::milliseconds{500})
            .pool_limits(64, 16)
            .health(60000, 120, "SELECT 1")
            .build();

    usub::uvent::system::co_spawn(pg.start_health_loop());

    server.handle("GET", "/api/v1/slow",
                  [&](usub::server::protocols::http::Request &,
                      usub::server::protocols::http::Response &res) -> usub::uvent::task::Awaitable<void> {
                      usub::ulog::debug("Incoming /slow");

                      usub::pg::RouteHint hint{
                          .kind = usub::pg::QueryKind::Read,
                          .consistency = usub::pg::Consistency::Eventual
                      };
                      auto pool = pg.route(hint);

                      auto r = co_await pool->query_awaitable("SELECT test.slow(10);");
                      if (!r.ok) {
                          usub::ulog::error("slow query failed: code={} | sqlstate='{}' | message='{}'",
                                            toString(r.code), r.err_detail.sqlstate, r.err_detail.message);
                          res.setStatus(500).addHeader("Content-Type", "application/json")
                                  .setBody(R"({"ok":false})");
                          co_return;
                      }

                      usub::ulog::debug("After /slow");
                      res.setStatus(200).addHeader("Content-Type", "application/json")
                              .setBody(R"({"ok":true})");
                      co_return;
                  });

    server.handle("GET", "/api/v1/slow_drop_db",
                  [&](usub::server::protocols::http::Request &,
                      usub::server::protocols::http::Response &res) -> usub::uvent::task::Awaitable<void> {
                      usub::ulog::debug("Incoming /slow_drop_db");

                      usub::pg::RouteHint hint{
                          .kind = usub::pg::QueryKind::Read,
                          .consistency = usub::pg::Consistency::Eventual
                      };
                      auto *pool = pg.route(hint);
                      if (!pool) {
                          usub::ulog::error("route(hint) returned null");
                          res.setStatus(503).addHeader("Content-Type", "application/json")
                                  .setBody(R"({"ok":false,"stage":"route1_null"})");
                          co_return;
                      }

                      auto rkill = co_await pool->query_awaitable(
                          "SELECT format('SELECT pg_terminate_backend(%s);', pg_backend_pid());"
                      );
                      if (!rkill.ok) {
                          usub::ulog::error("kill cmd query failed: code={} | sqlstate='{}' | message='{}'",
                                            toString(rkill.code), rkill.err_detail.sqlstate, rkill.err_detail.message);
                          res.setStatus(500).addHeader("Content-Type", "application/json")
                                  .setBody(R"({"ok":false,"stage":"kill_cmd"})");
                          co_return;
                      }

                      std::string kill_sql; {
                          kill_sql = rkill.at(0).at(0);
                      }

                      if (kill_sql.empty()) {
                          usub::ulog::error("cannot parse kill_sql from result");
                          res.setStatus(500).addHeader("Content-Type", "application/json")
                                  .setBody(R"({"ok":false,"stage":"kill_sql_parse"})");
                          co_return;
                      }

                      usub::ulog::info("kill_sql='{}'", kill_sql);

                      auto r = co_await pool->query_awaitable("SELECT pg_sleep(10);");
                      if (!r.ok) {
                          usub::ulog::error("slow_drop_db failed (expected): code={} | sqlstate='{}' | message='{}'",
                                            toString(r.code), r.err_detail.sqlstate, r.err_detail.message);
                          res.setStatus(500).addHeader("Content-Type", "application/json")
                                  .setBody(R"({"ok":false,"dropped_by_db":true})");
                          co_return;
                      }

                      usub::ulog::warn("slow_drop_db unexpectedly succeeded");
                      res.setStatus(200).addHeader("Content-Type", "application/json")
                              .setBody(R"({"ok":true,"dropped_by_db":false})");
                      co_return;
                  });

    server.run();
    return 0;
}
