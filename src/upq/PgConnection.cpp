#include "upq/PgConnection.h"

namespace usub::pg
{
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
                usub::uvent::net::Role::ACTIVE>>(fd);

        while (true)
        {
            PostgresPollingStatusType st = PQconnectPoll(this->conn_);

            if (st == PGRES_POLLING_OK)
                break;
            if (st == PGRES_POLLING_FAILED)
                co_return std::optional<std::string>{PQerrorMessage(this->conn_)};

            if (st == PGRES_POLLING_READING)
                co_await wait_readable();
            else if (st == PGRES_POLLING_WRITING)
                co_await wait_writable();
            else
                co_await usub::uvent::system::this_coroutine::sleep_for(std::chrono::milliseconds(1));
        }

        connected_ = true;

        co_return std::nullopt;
    }

    bool PgConnectionLibpq::connected() const noexcept
    {
        return connected_ && this->conn_ && (PQstatus(this->conn_) == CONNECTION_OK);
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::exec_simple_query_nonblocking(const std::string& sql)
    {
        QueryResult out;
        out.ok = false;

        if (!connected())
        {
            out.error = "connection not OK";
            co_return out;
        }

        if (!PQsendQuery(this->conn_, sql.c_str()))
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
                }
                else
                {
                    const char* err = PQresultErrorMessage(res);
                    if (err && *err)
                        out.error = err;
                    out.ok = false;
                }

                PQclear(res);
            }

            if (PQisBusy(this->conn_))
                co_await wait_readable();
        }
    }

    usub::uvent::task::Awaitable<void> PgConnectionLibpq::wait_readable()
    {
        co_await usub::uvent::net::detail::AwaiterRead{this->sock_->get_raw_header()};
        co_return;
    }

    usub::uvent::task::Awaitable<void> PgConnectionLibpq::wait_writable()
    {
        co_await usub::uvent::net::detail::AwaiterWrite{this->sock_->get_raw_header()};
        co_return;
    }
}
