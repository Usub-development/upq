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

        friend class PgNotificationMultiplexer;

        friend class PgPool;

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

        usub::uvent::task::Awaitable<PgCopyResult>
        copy_in_start(const std::string& sql);

        usub::uvent::task::Awaitable<PgCopyResult>
        copy_in_send_chunk(const void* data, size_t len);

        usub::uvent::task::Awaitable<PgCopyResult>
        copy_in_finish();

        usub::uvent::task::Awaitable<PgCopyResult>
        copy_out_start(const std::string& sql);

        usub::uvent::task::Awaitable<PgWireResult<std::vector<uint8_t>>>
        copy_out_read_chunk();

        std::string make_cursor_name();

        usub::uvent::task::Awaitable<QueryResult>
        cursor_declare(const std::string& cursor_name,
                       const std::string& sql);

        usub::uvent::task::Awaitable<PgCursorChunk>
        cursor_fetch_chunk(const std::string& cursor_name,
                           uint32_t count);

        usub::uvent::task::Awaitable<QueryResult>
        cursor_close(const std::string& cursor_name);

        PGconn* raw_conn() noexcept;

        bool is_idle();

    private:
        usub::uvent::task::Awaitable<void> wait_readable();
        usub::uvent::task::Awaitable<void> wait_writable();
        usub::uvent::task::Awaitable<void> wait_readable_for_listener();

        usub::uvent::task::Awaitable<bool> flush_outgoing();
        usub::uvent::task::Awaitable<bool> pump_input();

        QueryResult drain_all_results();
        PgCopyResult drain_copy_end_result();
        PgCursorChunk drain_single_result_rows();
        QueryResult drain_single_result_status_only();

    private:
        PGconn* conn_{nullptr};
        bool connected_{false};

        std::unique_ptr<
            usub::uvent::net::Socket<
                usub::uvent::net::Proto::TCP,
                usub::uvent::net::Role::ACTIVE
            >
        > sock_;

        uint64_t cursor_seq_{0};
    };

    inline void fill_server_error_fields(PGresult* res, QueryResult& out)
    {
        if (!res)
            return;

        const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        const char* primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char* detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        const char* hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);

        if (primary && *primary)
        {
            out.error = primary;
        }
        else
        {
            const char* fallback = PQresultErrorMessage(res);
            if (fallback && *fallback)
                out.error = fallback;
        }

        if (sqlstate) out.err_detail.sqlstate = sqlstate;
        if (detail) out.err_detail.detail = detail;
        if (hint) out.err_detail.hint = hint;

        if (primary && *primary)
            out.err_detail.message = primary;
        else if (!out.error.empty())
            out.err_detail.message = out.error;

        out.err_detail.category = classify_sqlstate(out.err_detail.sqlstate);

        out.ok = false;
        out.code = PgErrorCode::ServerError;
        out.rows_valid = false;
    }

    inline void fill_server_error_fields_copy(PGresult* res, PgCopyResult& out)
    {
        if (!res)
            return;

        const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        const char* primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char* detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        const char* hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);

        if (primary && *primary)
        {
            out.error = primary;
        }
        else
        {
            const char* fallback = PQresultErrorMessage(res);
            if (fallback && *fallback)
                out.error = fallback;
        }

        if (sqlstate) out.err_detail.sqlstate = sqlstate;
        if (detail) out.err_detail.detail = detail;
        if (hint) out.err_detail.hint = hint;

        if (primary && *primary)
            out.err_detail.message = primary;
        else if (!out.error.empty())
            out.err_detail.message = out.error;

        out.err_detail.category = classify_sqlstate(out.err_detail.sqlstate);

        out.ok = false;
        out.code = PgErrorCode::ServerError;
    }

    inline void fill_server_error_fields_cursor(PGresult* res, PgCursorChunk& out)
    {
        if (!res)
            return;

        const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        const char* primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char* detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        const char* hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);

        if (primary && *primary)
        {
            out.error = primary;
        }
        else
        {
            const char* fallback = PQresultErrorMessage(res);
            if (fallback && *fallback)
                out.error = fallback;
        }

        if (sqlstate) out.err_detail.sqlstate = sqlstate;
        if (detail) out.err_detail.detail = detail;
        if (hint) out.err_detail.hint = hint;

        if (primary && *primary)
            out.err_detail.message = primary;
        else if (!out.error.empty())
            out.err_detail.message = out.error;

        out.err_detail.category = classify_sqlstate(out.err_detail.sqlstate);

        out.ok = false;
        out.code = PgErrorCode::ServerError;
    }

    template <typename... Args>
    uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::exec_param_query_nonblocking(const std::string& sql, Args&&... args)
    {
        QueryResult out;
        out.ok = false;
        out.code = PgErrorCode::Unknown;
        out.rows_valid = true;

        if (!connected())
        {
            out.ok = false;
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            out.rows_valid = false;
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
                types[idx] = 25; // TEXT
                lengths[idx] = 0;
                formats[idx] = 0;
            }
            ++idx;
        };

        (make_val(std::forward<Args>(args)), ...);

        if (!PQsendQueryParams(this->conn_,
                               sql.c_str(),
                               N,
                               types,
                               values,
                               lengths,
                               formats,
                               0))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            out.rows_valid = false;
            co_return out;
        }

        while (true)
        {
            int fr = PQflush(this->conn_);
            if (fr == 0)
                break;
            if (fr == -1)
            {
                out.ok = false;
                out.code = PgErrorCode::SocketReadFailed;
                out.error = PQerrorMessage(this->conn_);
                out.rows_valid = false;
                co_return out;
            }
            co_await wait_writable();
        }

        while (true)
        {
            if (PQconsumeInput(this->conn_) == 0)
            {
                out.ok = false;
                out.code = PgErrorCode::SocketReadFailed;
                out.error = PQerrorMessage(this->conn_);
                out.rows_valid = false;
                co_return out;
            }

            while (!PQisBusy(this->conn_))
            {
                PGresult* res = PQgetResult(this->conn_);
                if (!res)
                {
                    if (out.error.empty())
                    {
                        out.ok = true;
                        out.code = PgErrorCode::OK;
                        out.rows_valid = true;
                    }
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
                            {
                                row.cols.emplace_back();
                            }
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
                    out.code = PgErrorCode::OK;
                    out.rows_valid = true;
                }
                else
                {
                    fill_server_error_fields(res, out);
                }

                PQclear(res);
            }

            if (PQisBusy(this->conn_))
                co_await wait_readable();
        }
    }
} // namespace usub::pg

#endif // PGCONNECTIONLIBPQ_H