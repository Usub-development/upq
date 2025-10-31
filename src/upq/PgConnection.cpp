#include "upq/PgConnection.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sstream>

namespace usub::pg
{
    // ---------- local helpers for COPY/CURSOR errors ----------
    static inline void fill_server_error_fields_copy(PGresult* res, PgCopyResult& out)
    {
        if (!res) return;
        const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        const char* primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char* detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        const char* hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);

        if (primary && *primary) out.error = primary;
        else if (const char* fb = PQresultErrorMessage(res); fb && *fb) out.error = fb;

        out.ok = false;
        out.code = PgErrorCode::ServerError;

        // optional: keep sqlstate in a generic text for diagnostics
        if (sqlstate && *sqlstate)
        {
            if (!out.error.empty())
            {
                out.error.append(" [SQLSTATE ").append(sqlstate).append("]");
            }
            else
            {
                out.error = std::string("SQLSTATE ") + sqlstate;
            }
        }
        if (detail && *detail) { out.error.append(" detail: ").append(detail); }
        if (hint && *hint) { out.error.append(" hint: ").append(hint); }
    }

    static inline void fill_server_error_fields_cursor(PGresult* res, PgCursorChunk& out)
    {
        if (!res) return;
        const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        const char* primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char* detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        const char* hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);

        if (primary && *primary) out.error = primary;
        else if (const char* fb = PQresultErrorMessage(res); fb && *fb) out.error = fb;

        out.ok = false;
        out.code = PgErrorCode::ServerError;
        out.done = true;

        if (sqlstate && *sqlstate)
        {
            if (!out.error.empty())
            {
                out.error.append(" [SQLSTATE ").append(sqlstate).append("]");
            }
            else
            {
                out.error = std::string("SQLSTATE ") + sqlstate;
            }
        }
        if (detail && *detail) { out.error.append(" detail: ").append(detail); }
        if (hint && *hint) { out.error.append(" hint: ").append(hint); }
    }

    // ---------- pgresult mappers ----------
    static inline QueryResult pgresult_to_QueryResult(PGresult* res)
    {
        QueryResult out;
        out.ok = false;
        out.code = PgErrorCode::Unknown;
        out.rows_valid = true;

        if (!res)
        {
            out.ok = true;
            out.code = PgErrorCode::OK;
            out.rows_valid = true;
            return out;
        }

        ExecStatusType st = PQresultStatus(res);
        if (st == PGRES_TUPLES_OK)
        {
            const int nrows = PQntuples(res);
            const int ncols = PQnfields(res);

            if (nrows > 0) out.rows.reserve(static_cast<size_t>(nrows));
            for (int r = 0; r < nrows; ++r)
            {
                QueryResult::Row row;
                row.cols.reserve(ncols);
                for (int c = 0; c < ncols; ++c)
                {
                    if (PQgetisnull(res, r, c))
                    {
                        row.cols.emplace_back();
                    }
                    else
                    {
                        const char* v = PQgetvalue(res, r, c);
                        const int len = PQgetlength(res, r, c);
                        row.cols.emplace_back(v, static_cast<size_t>(len));
                    }
                }
                out.rows.emplace_back(std::move(row));
            }

            out.ok = true;
            out.code = PgErrorCode::OK;
            out.rows_valid = true;
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

        return out;
    }

    static inline PgCopyResult pgresult_to_PgCopyResult(PGresult* res)
    {
        PgCopyResult out;
        out.ok = false;
        out.code = PgErrorCode::Unknown;
        out.rows_affected = 0;

        if (!res)
        {
            out.ok = true;
            out.code = PgErrorCode::OK;
            return out;
        }

        ExecStatusType st = PQresultStatus(res);
        if (st == PGRES_COMMAND_OK)
        {
            out.ok = true;
            out.code = PgErrorCode::OK;
            if (const char* aff = PQcmdTuples(res); aff && *aff)
            {
                out.rows_affected = std::strtoull(aff, nullptr, 10);
            }
        }
        else
        {
            fill_server_error_fields_copy(res, out);
        }

        return out;
    }

    static inline PgCursorChunk pgresult_to_PgCursorChunk(PGresult* res)
    {
        PgCursorChunk out;
        out.ok = false;
        out.code = PgErrorCode::Unknown;
        out.done = false;

        if (!res)
        {
            out.ok = true;
            out.code = PgErrorCode::OK;
            out.done = true;
            return out;
        }

        ExecStatusType st = PQresultStatus(res);
        if (st == PGRES_TUPLES_OK)
        {
            const int nrows = PQntuples(res);
            const int ncols = PQnfields(res);

            if (nrows > 0) out.rows.reserve(nrows);
            for (int r = 0; r < nrows; ++r)
            {
                QueryResult::Row row;
                row.cols.reserve(ncols);
                for (int c = 0; c < ncols; ++c)
                {
                    if (PQgetisnull(res, r, c))
                    {
                        row.cols.emplace_back();
                    }
                    else
                    {
                        const char* v = PQgetvalue(res, r, c);
                        const int len = PQgetlength(res, r, c);
                        row.cols.emplace_back(v, static_cast<size_t>(len));
                    }
                }
                out.rows.emplace_back(std::move(row));
            }

            if (nrows == 0) out.done = true;

            out.ok = true;
            out.code = PgErrorCode::OK;
        }
        else if (st == PGRES_COMMAND_OK)
        {
            out.ok = true;
            out.code = PgErrorCode::OK;
            out.done = true;
        }
        else
        {
            fill_server_error_fields_cursor(res, out);
        }

        return out;
    }

    // ---------- ctor / dtor ----------
    PgConnectionLibpq::PgConnectionLibpq() = default;

    PgConnectionLibpq::~PgConnectionLibpq()
    {
        if (sock_) sock_.reset();
        if (conn_)
        {
            PQfinish(conn_);
            conn_ = nullptr;
        }
    }

    // ---------- connect ----------
    usub::uvent::task::Awaitable<std::optional<std::string>>
    PgConnectionLibpq::connect_async(const std::string& conninfo)
    {
        conn_ = PQconnectStart(conninfo.c_str());
        if (!conn_) co_return std::optional<std::string>{"PQconnectStart failed"};

        if (PQstatus(conn_) == CONNECTION_BAD)
            co_return std::optional<std::string>{PQerrorMessage(conn_)};

        if (PQsetnonblocking(conn_, 1) != 0)
            co_return std::optional<std::string>{"PQsetnonblocking failed"};

        const int fd = PQsocket(conn_);
        if (fd < 0)
            co_return std::optional<std::string>{"PQsocket <0"};

        sock_ = std::make_unique<
            usub::uvent::net::Socket<
                usub::uvent::net::Proto::TCP,
                usub::uvent::net::Role::ACTIVE
            >
        >(fd);

        for (;;)
        {
            const auto st = PQconnectPoll(conn_);
            if (st == PGRES_POLLING_OK) break;
            if (st == PGRES_POLLING_FAILED) co_return std::optional<std::string>{PQerrorMessage(conn_)};
            if (st == PGRES_POLLING_READING)
            {
                co_await wait_readable();
                continue;
            }
            if (st == PGRES_POLLING_WRITING)
            {
                co_await wait_writable();
                continue;
            }

            co_await usub::uvent::system::this_coroutine::sleep_for(std::chrono::milliseconds(1));
        }

        connected_ = true;
        co_return std::nullopt;
    }

    bool PgConnectionLibpq::connected() const noexcept
    {
        return connected_ && conn_ && (PQstatus(conn_) == CONNECTION_OK);
    }

    // ---------- I/O pumps ----------
    usub::uvent::task::Awaitable<bool> PgConnectionLibpq::flush_outgoing()
    {
        for (;;)
        {
            const int fr = PQflush(conn_);
            if (fr == 0) co_return true;
            if (fr == -1) co_return false;
            co_await wait_writable();
        }
    }

    usub::uvent::task::Awaitable<bool> PgConnectionLibpq::pump_input()
    {
        for (;;)
        {
            if (PQconsumeInput(conn_) == 0) co_return false;
            if (!PQisBusy(conn_)) co_return true;
            co_await wait_readable();
        }
    }

    // ---------- drains ----------
    QueryResult PgConnectionLibpq::drain_all_results()
    {
        QueryResult final_out;
        final_out.ok = true;
        final_out.code = PgErrorCode::OK;
        final_out.rows_valid = true;

        while (PGresult* res = PQgetResult(conn_))
        {
            QueryResult tmp = pgresult_to_QueryResult(res);
            PQclear(res);

            if (!tmp.ok)
            {
                final_out = std::move(tmp);
            }
            else if (tmp.rows_valid && !tmp.rows.empty())
            {
                // append rows
                final_out.rows.reserve(final_out.rows.size() + tmp.rows.size());
                for (auto& r : tmp.rows) final_out.rows.emplace_back(std::move(r));
            }
        }

        if (!final_out.ok) final_out.rows_valid = false;
        return final_out;
    }

    PgCopyResult PgConnectionLibpq::drain_copy_end_result()
    {
        PgCopyResult out;
        out.ok = true;
        out.code = PgErrorCode::OK;
        out.rows_affected = 0;

        while (PGresult* res = PQgetResult(conn_))
        {
            PgCopyResult tmp = pgresult_to_PgCopyResult(res);
            PQclear(res);

            if (!tmp.ok)
            {
                out = std::move(tmp);
            }
            else
            {
                out.rows_affected += tmp.rows_affected;
            }
        }
        return out;
    }

    PgCursorChunk PgConnectionLibpq::drain_single_result_rows()
    {
        if (PGresult* res = PQgetResult(conn_))
        {
            PgCursorChunk chunk = pgresult_to_PgCursorChunk(res);
            PQclear(res);

            if (PGresult* leftover = PQgetResult(conn_))
            {
                PgCursorChunk err{};
                fill_server_error_fields_cursor(leftover, err);
                PQclear(leftover);
                return err;
            }
            return chunk;
        }

        PgCursorChunk out{};
        out.ok = true;
        out.code = PgErrorCode::OK;
        out.done = true;
        return out;
    }

    QueryResult PgConnectionLibpq::drain_single_result_status_only()
    {
        if (PGresult* res = PQgetResult(conn_))
        {
            QueryResult tmp = pgresult_to_QueryResult(res);
            PQclear(res);

            if (PGresult* leftover = PQgetResult(conn_))
            {
                QueryResult err{};
                fill_server_error_fields(leftover, err);
                PQclear(leftover);
                err.rows_valid = false;
                return err;
            }

            tmp.rows.clear();
            tmp.rows_valid = true;
            return tmp;
        }

        QueryResult ok{};
        ok.ok = true;
        ok.code = PgErrorCode::OK;
        ok.rows_valid = true;
        return ok;
    }

    // ---------- simple query ----------
    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::exec_simple_query_nonblocking(const std::string& sql)
    {
        QueryResult out{};
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

        if (!PQsendQuery(conn_, sql.c_str()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            out.rows_valid = false;
            co_return out;
        }

        if (!(co_await flush_outgoing()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            out.rows_valid = false;
            co_return out;
        }

        if (!(co_await pump_input()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            out.rows_valid = false;
            co_return out;
        }

        co_return drain_all_results();
    }

    // ---------- COPY IN ----------
    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_in_start(const std::string& sql)
    {
        PgCopyResult out{};
        out.ok = false;
        out.code = PgErrorCode::Unknown;

        if (!connected())
        {
            out.ok = false;
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            co_return out;
        }

        if (!PQsendQuery(conn_, sql.c_str()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            co_return out;
        }

        if (!(co_await flush_outgoing()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            co_return out;
        }

        if (!(co_await pump_input()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            co_return out;
        }

        PGresult* res = PQgetResult(conn_);
        if (!res)
        {
            out.ok = false;
            out.code = PgErrorCode::ProtocolCorrupt;
            out.error = "no result after COPY start";
            co_return out;
        }

        if (PQresultStatus(res) != PGRES_COPY_IN)
        {
            fill_server_error_fields_copy(res, out);
            PQclear(res);
            co_return out;
        }

        out.ok = true;
        out.code = PgErrorCode::OK;
        PQclear(res);
        co_return out;
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_in_send_chunk(const void* data, size_t len)
    {
        PgCopyResult out{};
        out.ok = false;
        out.code = PgErrorCode::Unknown;

        if (!connected())
        {
            out.ok = false;
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            co_return out;
        }

        if (PQputCopyData(conn_, reinterpret_cast<const char*>(data), static_cast<int>(len)) != 1)
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            co_return out;
        }

        if (!(co_await flush_outgoing()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            co_return out;
        }

        out.ok = true;
        out.code = PgErrorCode::OK;
        co_return out;
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_in_finish()
    {
        PgCopyResult out{};
        out.ok = false;
        out.code = PgErrorCode::Unknown;
        out.rows_affected = 0;

        if (!connected())
        {
            out.ok = false;
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            co_return out;
        }

        if (PQputCopyEnd(conn_, nullptr) != 1)
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            co_return out;
        }

        if (!(co_await flush_outgoing()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            co_return out;
        }

        if (!(co_await pump_input()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            co_return out;
        }

        co_return drain_copy_end_result();
    }

    // ---------- COPY OUT ----------
    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_out_start(const std::string& sql)
    {
        PgCopyResult out{};
        out.ok = false;
        out.code = PgErrorCode::Unknown;
        out.rows_affected = 0;

        if (!connected())
        {
            out.ok = false;
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            co_return out;
        }

        if (!PQsendQuery(conn_, sql.c_str()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            co_return out;
        }

        if (!(co_await flush_outgoing()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            co_return out;
        }

        if (!(co_await pump_input()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            co_return out;
        }

        PGresult* res = PQgetResult(conn_);
        if (!res)
        {
            out.ok = false;
            out.code = PgErrorCode::ProtocolCorrupt;
            out.error = "no result after COPY start";
            co_return out;
        }

        if (PQresultStatus(res) != PGRES_COPY_OUT)
        {
            fill_server_error_fields_copy(res, out);
            PQclear(res);
            co_return out;
        }

        out.ok = true;
        out.code = PgErrorCode::OK;
        PQclear(res);
        co_return out;
    }

    usub::uvent::task::Awaitable<PgWireResult<std::vector<uint8_t>>>
    PgConnectionLibpq::copy_out_read_chunk()
    {
        PgWireResult<std::vector<uint8_t>> out{};
        out.ok = false;
        out.err.code = PgErrorCode::Unknown;

        if (!connected())
        {
            out.ok = false;
            out.err.code = PgErrorCode::ConnectionClosed;
            out.err.message = "connection not OK";
            co_return out;
        }

        // Ensure input is pumped; avoid recursion
        for (;;)
        {
            if (!PQisBusy(conn_)) break;
            if (!(co_await pump_input()))
            {
                out.ok = false;
                out.err.code = PgErrorCode::SocketReadFailed;
                out.err.message = PQerrorMessage(conn_);
                co_return out;
            }
        }

        for (;;)
        {
            char* buf = nullptr;
            const int rc = PQgetCopyData(conn_, &buf, 0);
            if (rc > 0)
            {
                out.value.resize(static_cast<size_t>(rc));
                std::memcpy(out.value.data(), buf, static_cast<size_t>(rc));
                PQfreemem(buf);
                out.ok = true;
                co_return out;
            }
            if (rc == 0)
            {
                co_await wait_readable();
                // loop and try again
                continue;
            }

            // rc < 0: COPY finished or error
            if (PGresult* res = PQgetResult(conn_))
            {
                if (PQresultStatus(res) == PGRES_COMMAND_OK)
                {
                    PQclear(res);
                    out.ok = true; // EOF marker: return ok with empty chunk
                    out.value.clear();
                    co_return out;
                }

                PgCopyResult tmp_err{};
                fill_server_error_fields_copy(res, tmp_err);
                PQclear(res);

                out.ok = false;
                out.err.code = tmp_err.code;
                out.err.message = tmp_err.error;
                co_return out;
            }

            // No result: protocol issue
            out.ok = false;
            out.err.code = PgErrorCode::ProtocolCorrupt;
            out.err.message = "COPY OUT finished but no result";
            co_return out;
        }
    }

    // ---------- cursors ----------
    std::string PgConnectionLibpq::make_cursor_name()
    {
        const uint64_t seq = ++cursor_seq_;
        char buf[64];
        std::snprintf(buf, sizeof(buf), "usub_cur_%llu", static_cast<unsigned long long>(seq));
        return std::string(buf);
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::cursor_declare(const std::string& cursor_name, const std::string& sql)
    {
        std::string full = "BEGIN; DECLARE " + cursor_name + " NO SCROLL CURSOR FOR " + sql + ";";
        co_return co_await exec_simple_query_nonblocking(full);
    }

    usub::uvent::task::Awaitable<PgCursorChunk>
    PgConnectionLibpq::cursor_fetch_chunk(const std::string& cursor_name, uint32_t count)
    {
        PgCursorChunk chunk{};
        chunk.ok = false;
        chunk.code = PgErrorCode::Unknown;

        if (!connected())
        {
            chunk.ok = false;
            chunk.code = PgErrorCode::ConnectionClosed;
            chunk.error = "connection not OK";
            co_return chunk;
        }

        char cntbuf[32];
        std::snprintf(cntbuf, sizeof(cntbuf), "%u", count);
        const std::string fetch_sql = "FETCH FORWARD " + std::string(cntbuf) + " FROM " + cursor_name + ";";

        if (!PQsendQuery(conn_, fetch_sql.c_str()))
        {
            chunk.ok = false;
            chunk.code = PgErrorCode::SocketReadFailed;
            chunk.error = PQerrorMessage(conn_);
            co_return chunk;
        }

        if (!(co_await flush_outgoing()))
        {
            chunk.ok = false;
            chunk.code = PgErrorCode::SocketReadFailed;
            chunk.error = PQerrorMessage(conn_);
            co_return chunk;
        }

        if (!(co_await pump_input()))
        {
            chunk.ok = false;
            chunk.code = PgErrorCode::SocketReadFailed;
            chunk.error = PQerrorMessage(conn_);
            co_return chunk;
        }

        co_return drain_single_result_rows();
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::cursor_close(const std::string& cursor_name)
    {
        QueryResult final{};
        final.ok = false;
        final.code = PgErrorCode::Unknown;
        final.rows_valid = true;

        if (!connected())
        {
            final.ok = false;
            final.code = PgErrorCode::ConnectionClosed;
            final.error = "connection not OK";
            final.rows_valid = false;
            co_return final;
        }

        const std::string close_sql = "CLOSE " + cursor_name + "; COMMIT;";
        if (!PQsendQuery(conn_, close_sql.c_str()))
        {
            final.ok = false;
            final.code = PgErrorCode::SocketReadFailed;
            final.error = PQerrorMessage(conn_);
            final.rows_valid = false;
            co_return final;
        }

        if (!(co_await flush_outgoing()))
        {
            final.ok = false;
            final.code = PgErrorCode::SocketReadFailed;
            final.error = PQerrorMessage(conn_);
            final.rows_valid = false;
            co_return final;
        }

        if (!(co_await pump_input()))
        {
            final.ok = false;
            final.code = PgErrorCode::SocketReadFailed;
            final.error = PQerrorMessage(conn_);
            final.rows_valid = false;
            co_return final;
        }

        final = drain_all_results();
        final.rows.clear(); // status only
        final.rows_valid = true;
        co_return final;
    }

    // ---------- misc ----------
    PGconn* PgConnectionLibpq::raw_conn() noexcept { return conn_; }

    bool PgConnectionLibpq::is_idle()
    {
        if (!connected()) return false;
        if (PQisBusy(conn_) != 0) return false;

        bool clean = true;
        while (PGresult* r = PQgetResult(conn_))
        {
            const auto st = PQresultStatus(r);
            if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) clean = false;
            PQclear(r);
        }
        return clean;
    }

    usub::uvent::task::Awaitable<void> PgConnectionLibpq::wait_readable()
    {
        co_await usub::uvent::net::detail::AwaiterRead{sock_->get_raw_header()};
        co_return;
    }

    usub::uvent::task::Awaitable<void> PgConnectionLibpq::wait_writable()
    {
        co_await usub::uvent::net::detail::AwaiterWrite{sock_->get_raw_header()};
        co_return;
    }

    usub::uvent::task::Awaitable<void> PgConnectionLibpq::wait_readable_for_listener()
    {
        co_await wait_readable();
        co_return;
    }
} // namespace usub::pg
