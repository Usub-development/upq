#ifndef PGCONNECTIONLIBPQ_H
#define PGCONNECTIONLIBPQ_H

#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <chrono>

#include <libpq-fe.h>

#include "uvent/Uvent.h"
#include "PgTypes.h"
#include "meta/PgConcepts.h"

namespace usub::pg
{
    class PgConnectionLibpq
    {
    public:
        template <class HandlerT>
        requires PgNotifyHandler<HandlerT>
        friend class PgNotificationListener;

        PgConnectionLibpq();

        ~PgConnectionLibpq();

        usub::uvent::task::Awaitable<std::optional<std::string>>
        connect_async(const std::string& conninfo);

        [[nodiscard]] bool connected() const noexcept;

        usub::uvent::task::Awaitable<QueryResult>
        exec_simple_query_nonblocking(const std::string& sql);

        template <typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        exec_param_query_nonblocking(const std::string& sql, Args&&... args);

        PGconn* raw_conn() noexcept;

    private:
        usub::uvent::task::Awaitable<void> wait_readable();
        usub::uvent::task::Awaitable<void> wait_writable();
        usub::uvent::task::Awaitable<void> wait_readable_for_listener();

    private:
        PGconn* conn_{nullptr};
        bool connected_{false};

        std::unique_ptr<
            usub::uvent::net::Socket<
                usub::uvent::net::Proto::TCP,
                usub::uvent::net::Role::ACTIVE
            >
        > sock_;
    };

    template <typename... Args>
    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::exec_param_query_nonblocking(const std::string& sql, Args&&... args)
    {
        QueryResult out;
        out.ok = false;

        if (!connected())
        {
            out.error = "connection not OK";
            co_return out;
        }

        constexpr size_t N = sizeof...(Args);
        const char* values[N];
        int lengths[N];
        int formats[N];
        Oid types[N];

        std::vector<std::string> temp;
        temp.reserve(N);

        size_t idx = 0;
        auto make_val = [&](auto&& arg)
        {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_integral_v<T>)
            {
                temp.emplace_back(std::to_string(arg));
                values[idx] = temp.back().c_str();
                types[idx] = 23;
                lengths[idx] = 0;
                formats[idx] = 0;
            }
            else if constexpr (std::is_floating_point_v<T>)
            {
                temp.emplace_back(std::to_string(arg));
                values[idx] = temp.back().c_str();
                types[idx] = 701;
                lengths[idx] = 0;
                formats[idx] = 0;
            }
            else
            {
                temp.emplace_back(std::string(arg));
                values[idx] = temp.back().c_str();
                types[idx] = 25;
                lengths[idx] = 0;
                formats[idx] = 0;
            }
            ++idx;
        };

        (make_val(std::forward<Args>(args)), ...);

        if (!PQsendQueryParams(this->conn_, sql.c_str(),
                               N, types, values, lengths, formats, 0))
        {
            out.error = PQerrorMessage(this->conn_);
            co_return out;
        }

        while (true)
        {
            int fr = PQflush(this->conn_);
            if (fr == 0) break;
            if (fr == -1)
            {
                out.error = PQerrorMessage(this->conn_);
                co_return out;
            }
            co_await wait_writable();
        }

        while (true)
        {
            if (PQconsumeInput(this->conn_) == 0)
            {
                out.error = PQerrorMessage(this->conn_);
                co_return out;
            }

            while (!PQisBusy(this->conn_))
            {
                PGresult* res = PQgetResult(this->conn_);
                if (!res)
                {
                    if (out.error.empty()) out.ok = true;
                    co_return out;
                }

                ExecStatusType st = PQresultStatus(res);
                if (st == PGRES_TUPLES_OK)
                {
                    int nrows = PQntuples(res);
                    int ncols = PQnfields(res);
                    for (int r = 0; r < nrows; r++)
                    {
                        QueryResult::Row row;
                        row.cols.reserve(ncols);
                        for (int c = 0; c < ncols; c++)
                        {
                            if (PQgetisnull(res, r, c))
                                row.cols.emplace_back();
                            else
                            {
                                const char* v = PQgetvalue(res, r, c);
                                int len = PQgetlength(res, r, c);
                                row.cols.emplace_back(v, (size_t)len);
                            }
                        }
                        out.rows.push_back(std::move(row));
                    }
                }
                else if (st == PGRES_COMMAND_OK)
                {
                    out.ok = true;
                }
                else
                {
                    const char* err = PQresultErrorMessage(res);
                    if (err && *err) out.error = err;
                    out.ok = false;
                }

                PQclear(res);
            }

            if (PQisBusy(this->conn_))
                co_await wait_readable();
        }
    }
} // namespace usub::pg

#endif // PGCONNECTIONLIBPQ_H
