#include "upq/PgConnection.h"

namespace usub::pg
{
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
            const char* aff = PQcmdTuples(res);
            if (aff && *aff)
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
            int nrows = PQntuples(res);
            int ncols = PQnfields(res);

            out.rows.reserve(nrows);

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

            if (nrows == 0)
            {
                out.done = true;
            }

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

    PgConnectionLibpq::PgConnectionLibpq() = default;

    PgConnectionLibpq::~PgConnectionLibpq()
    {
        if (this->sock_)
            this->sock_.reset();

        if (this->conn_)
        {
            PQfinish(this->conn_);
            conn_ = nullptr;
        }
    }

    usub::uvent::task::Awaitable<std::optional<std::string>>
    PgConnectionLibpq::connect_async(const std::string& conninfo)
    {
        this->conn_ = PQconnectStart(conninfo.c_str());
        if (!this->conn_)
            co_return std::optional<std::string>{"PQconnectStart failed"};

        if (PQstatus(this->conn_) == CONNECTION_BAD)
            co_return std::optional<std::string>{PQerrorMessage(this->conn_)};

        if (PQsetnonblocking(this->conn_, 1) != 0)
            co_return std::optional<std::string>{"PQsetnonblocking failed"};

        int fd = PQsocket(this->conn_);
        if (fd < 0)
            co_return std::optional<std::string>{"PQsocket <0"};

        this->sock_ = std::make_unique<
            usub::uvent::net::Socket<
                usub::uvent::net::Proto::TCP,
                usub::uvent::net::Role::ACTIVE
            >
        >(fd);

        while (true)
        {
            PostgresPollingStatusType st = PQconnectPoll(this->conn_);

            if (st == PGRES_POLLING_OK)
                break;
            if (st == PGRES_POLLING_FAILED)
                co_return std::optional<std::string>{PQerrorMessage(this->conn_)};

            if (st == PGRES_POLLING_READING)
            {
                co_await wait_readable();
            }
            else if (st == PGRES_POLLING_WRITING)
            {
                co_await wait_writable();
            }
            else
            {
                co_await usub::uvent::system::this_coroutine::sleep_for(
                    std::chrono::milliseconds(1)
                );
            }
        }

        connected_ = true;
        co_return std::nullopt;
    }

    bool PgConnectionLibpq::connected() const noexcept
    {
        return connected_
            && this->conn_
            && (PQstatus(this->conn_) == CONNECTION_OK);
    }

    usub::uvent::task::Awaitable<bool> PgConnectionLibpq::flush_outgoing()
    {
        while (true)
        {
            int fr = PQflush(this->conn_);
            if (fr == 0)
                co_return true;
            if (fr == -1)
                co_return false;
            co_await wait_writable();
        }
    }

    usub::uvent::task::Awaitable<bool> PgConnectionLibpq::pump_input()
    {
        while (true)
        {
            if (PQconsumeInput(this->conn_) == 0)
                co_return false;

            if (!PQisBusy(this->conn_))
                co_return true;

            co_await wait_readable();
        }
    }

    QueryResult PgConnectionLibpq::drain_all_results()
    {
        QueryResult final_out;
        final_out.ok = true;
        final_out.code = PgErrorCode::OK;
        final_out.rows_valid = true;

        while (true)
        {
            PGresult* res = PQgetResult(this->conn_);
            if (!res)
                break;

            QueryResult tmp = pgresult_to_QueryResult(res);
            PQclear(res);

            if (!tmp.ok)
            {
                final_out = tmp;
            }
            else
            {
                if (tmp.rows_valid && !tmp.rows.empty())
                {
                    for (auto& r : tmp.rows)
                        final_out.rows.push_back(std::move(r));
                }
            }
        }

        if (!final_out.ok)
        {
            final_out.rows_valid = false;
        }

        return final_out;
    }

    PgCopyResult PgConnectionLibpq::drain_copy_end_result()
    {
        PgCopyResult out;
        out.ok = true;
        out.code = PgErrorCode::OK;

        while (true)
        {
            PGresult* res = PQgetResult(this->conn_);
            if (!res)
                break;

            PgCopyResult tmp = pgresult_to_PgCopyResult(res);
            PQclear(res);

            if (!tmp.ok)
            {
                out = tmp;
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
        PGresult* res = PQgetResult(this->conn_);
        if (!res)
        {
            PgCursorChunk out;
            out.ok = true;
            out.code = PgErrorCode::OK;
            out.done = true;
            return out;
        }

        PgCursorChunk chunk = pgresult_to_PgCursorChunk(res);
        PQclear(res);

        PGresult* leftover = PQgetResult(this->conn_);
        if (leftover)
        {
            PgCursorChunk err;
            fill_server_error_fields_cursor(leftover, err);
            PQclear(leftover);
            return err;
        }

        return chunk;
    }

    QueryResult PgConnectionLibpq::drain_single_result_status_only()
    {
        PGresult* res = PQgetResult(this->conn_);
        if (!res)
        {
            QueryResult ok;
            ok.ok = true;
            ok.code = PgErrorCode::OK;
            ok.rows_valid = true;
            return ok;
        }

        QueryResult tmp = pgresult_to_QueryResult(res);
        PQclear(res);

        PGresult* leftover = PQgetResult(this->conn_);
        if (leftover)
        {
            QueryResult err;
            fill_server_error_fields(leftover, err);
            PQclear(leftover);
            err.rows_valid = false;
            return err;
        }

        tmp.rows.clear();
        tmp.rows_valid = true;
        return tmp;
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::exec_simple_query_nonblocking(const std::string& sql)
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

        if (!PQsendQuery(this->conn_, sql.c_str()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            out.rows_valid = false;
            co_return out;
        }

        if (!(co_await flush_outgoing()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            out.rows_valid = false;
            co_return out;
        }

        if (!(co_await pump_input()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            out.rows_valid = false;
            co_return out;
        }

        QueryResult final_out = drain_all_results();
        co_return final_out;
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_in_start(const std::string& sql)
    {
        PgCopyResult out;
        out.ok = false;
        out.code = PgErrorCode::Unknown;

        if (!connected())
        {
            out.ok = false;
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            co_return out;
        }

        if (!PQsendQuery(this->conn_, sql.c_str()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            co_return out;
        }

        if (!(co_await flush_outgoing()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            co_return out;
        }

        if (!(co_await pump_input()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            co_return out;
        }

        PGresult* res = PQgetResult(this->conn_);
        if (!res)
        {
            out.ok = false;
            out.code = PgErrorCode::ProtocolCorrupt;
            out.error = "no result after COPY start";
            co_return out;
        }

        ExecStatusType st = PQresultStatus(res);
        if (st != PGRES_COPY_IN)
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
        PgCopyResult out;
        out.ok = false;
        out.code = PgErrorCode::Unknown;

        if (!connected())
        {
            out.ok = false;
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            co_return out;
        }

        int rc = PQputCopyData(this->conn_,
                               reinterpret_cast<const char*>(data),
                               (int)len);
        if (rc != 1)
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            co_return out;
        }

        if (!(co_await flush_outgoing()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            co_return out;
        }

        out.ok = true;
        out.code = PgErrorCode::OK;
        co_return out;
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_in_finish()
    {
        PgCopyResult out;
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

        int rc = PQputCopyEnd(this->conn_, nullptr);
        if (rc != 1)
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            co_return out;
        }

        if (!(co_await flush_outgoing()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            co_return out;
        }

        if (!(co_await pump_input()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            co_return out;
        }

        out = drain_copy_end_result();
        co_return out;
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_out_start(const std::string& sql)
    {
        PgCopyResult out;
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

        if (!PQsendQuery(this->conn_, sql.c_str()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            co_return out;
        }

        if (!(co_await flush_outgoing()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            co_return out;
        }

        if (!(co_await pump_input()))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(this->conn_);
            co_return out;
        }

        PGresult* res = PQgetResult(this->conn_);
        if (!res)
        {
            out.ok = false;
            out.code = PgErrorCode::ProtocolCorrupt;
            out.error = "no result after COPY start";
            co_return out;
        }

        ExecStatusType st = PQresultStatus(res);
        if (st != PGRES_COPY_OUT)
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
        PgWireResult<std::vector<uint8_t>> out;
        out.ok = false;
        out.err.code = PgErrorCode::Unknown;
        out.err.message.clear();

        if (!connected())
        {
            out.ok = false;
            out.err.code = PgErrorCode::ConnectionClosed;
            out.err.message = "connection not OK";
            co_return out;
        }

        while (true)
        {
            int is_busy = PQisBusy(this->conn_);
            if (!is_busy)
                break;
            if (!(co_await pump_input()))
            {
                out.ok = false;
                out.err.code = PgErrorCode::SocketReadFailed;
                out.err.message = PQerrorMessage(this->conn_);
                co_return out;
            }
        }

        char* buf = nullptr;
        int len = 0;
        int rc = PQgetCopyData(this->conn_, &buf, 0);
        if (rc > 0)
        {
            out.value.resize((size_t)rc);
            std::memcpy(out.value.data(), buf, (size_t)rc);
            PQfreemem(buf);

            out.ok = true;
            co_return out;
        }
        else if (rc == 0)
        {
            co_await wait_readable();
            co_return co_await copy_out_read_chunk();
        }
        else
        {
            PGresult* res = PQgetResult(this->conn_);
            if (!res)
            {
                out.ok = false;
                out.err.code = PgErrorCode::ProtocolCorrupt;
                out.err.message = "COPY OUT finished but no result";
                co_return out;
            }

            ExecStatusType st = PQresultStatus(res);
            if (st == PGRES_COMMAND_OK)
            {
                out.ok = true;
                out.value.clear();
                PQclear(res);
                co_return out;
            }

            PgCopyResult tmp_err;
            fill_server_error_fields_copy(res, tmp_err);
            PQclear(res);

            out.ok = false;
            out.err.code = tmp_err.code;
            out.err.message = tmp_err.error;
            co_return out;
        }
    }

    std::string PgConnectionLibpq::make_cursor_name()
    {
        uint64_t seq = ++cursor_seq_;
        char buf[64];
        std::snprintf(buf, sizeof(buf), "usub_cur_%llu", (unsigned long long)seq);
        return std::string(buf);
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::cursor_declare(const std::string& cursor_name,
                                      const std::string& sql)
    {
        std::string full = "BEGIN; DECLARE " + cursor_name + " NO SCROLL CURSOR FOR " + sql + ";";
        QueryResult qr = co_await exec_simple_query_nonblocking(full);
        co_return qr;
    }

    usub::uvent::task::Awaitable<PgCursorChunk>
    PgConnectionLibpq::cursor_fetch_chunk(const std::string& cursor_name,
                                          uint32_t count)
    {
        PgCursorChunk chunk;
        chunk.ok = false;
        chunk.code = PgErrorCode::Unknown;

        if (!connected())
        {
            chunk.ok = false;
            chunk.code = PgErrorCode::ConnectionClosed;
            chunk.error = "connection not OK";
            co_return chunk;
        }

        {
            char cntbuf[32];
            std::snprintf(cntbuf, sizeof(cntbuf), "%u", count);
            std::string fetch_sql = "FETCH FORWARD " + std::string(cntbuf) + " FROM " + cursor_name + ";";

            if (!PQsendQuery(this->conn_, fetch_sql.c_str()))
            {
                chunk.ok = false;
                chunk.code = PgErrorCode::SocketReadFailed;
                chunk.error = PQerrorMessage(this->conn_);
                co_return chunk;
            }

            bool flushed = co_await flush_outgoing();
            if (!flushed)
            {
                chunk.ok = false;
                chunk.code = PgErrorCode::SocketReadFailed;
                chunk.error = PQerrorMessage(this->conn_);
                co_return chunk;
            }

            bool pumped = co_await pump_input();
            if (!pumped)
            {
                chunk.ok = false;
                chunk.code = PgErrorCode::SocketReadFailed;
                chunk.error = PQerrorMessage(this->conn_);
                co_return chunk;
            }
        }

        PgCursorChunk res_chunk = drain_single_result_rows();
        co_return res_chunk;
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::cursor_close(const std::string& cursor_name)
    {
        QueryResult final;
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

        std::string close_sql = "CLOSE " + cursor_name + "; COMMIT;";

        if (!PQsendQuery(this->conn_, close_sql.c_str()))
        {
            final.ok = false;
            final.code = PgErrorCode::SocketReadFailed;
            final.error = PQerrorMessage(this->conn_);
            final.rows_valid = false;
            co_return final;
        }

        bool flushed = co_await flush_outgoing();
        if (!flushed)
        {
            final.ok = false;
            final.code = PgErrorCode::SocketReadFailed;
            final.error = PQerrorMessage(this->conn_);
            final.rows_valid = false;
            co_return final;
        }

        bool pumped = co_await pump_input();
        if (!pumped)
        {
            final.ok = false;
            final.code = PgErrorCode::SocketReadFailed;
            final.error = PQerrorMessage(this->conn_);
            final.rows_valid = false;
            co_return final;
        }

        final = drain_all_results();
        final.rows.clear();
        final.rows_valid = true;

        co_return final;
    }

    PGconn* PgConnectionLibpq::raw_conn() noexcept
    {
        return this->conn_;
    }

    bool PgConnectionLibpq::is_idle()
    {
        if (!connected())
            return false;

        if (PQisBusy(this->conn_) != 0)
            return false;

        bool clean = true;
        while (true)
        {
            PGresult* r = PQgetResult(this->conn_);
            if (!r)
                break;

            ExecStatusType st = PQresultStatus(r);
            if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK)
            {
                clean = false;
            }

            PQclear(r);
        }

        return clean;
    }

    usub::uvent::task::Awaitable<void> PgConnectionLibpq::wait_readable()
    {
        co_await usub::uvent::net::detail::AwaiterRead{
            this->sock_->get_raw_header()
        };
        co_return;
    }

    usub::uvent::task::Awaitable<void> PgConnectionLibpq::wait_writable()
    {
        co_await usub::uvent::net::detail::AwaiterWrite{
            this->sock_->get_raw_header()
        };
        co_return;
    }

    usub::uvent::task::Awaitable<void> PgConnectionLibpq::wait_readable_for_listener()
    {
        co_await wait_readable();
        co_return;
    }
} // namespace usub::pg
