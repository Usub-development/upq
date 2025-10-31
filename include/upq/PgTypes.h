#ifndef PGTYPES_H
#define PGTYPES_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <coroutine>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <openssl/md5.h>

#include "uvent/Uvent.h"
#include "uvent/utils/sync/RefCountedSession.h"

namespace usub::pg
{
    enum class PgErrorCode : uint32_t
    {
        OK = 0,
        InvalidFuture,
        ConnectionClosed,
        SocketReadFailed,
        ProtocolCorrupt,
        ParserTruncatedField,
        ParserTruncatedRow,
        ParserTruncatedHeader,
        ServerError,
        AuthFailed,
        AwaitCanceled,
        Unknown
    };

    // High-level server-side error classification, derived from SQLSTATE.
    enum class PgSqlStateClass : uint8_t
    {
        None = 0, // no sqlstate / not an error
        ConnectionError, // 08***, connection failures etc.
        SyntaxError, // 42*** (syntax/access rule)
        UndefinedObject, // e.g. 42P01 (relation does not exist)
        ConstraintViolation, // 23*** generic integrity violation
        UniqueViolation, // 23505
        CheckViolation, // 23514
        NotNullViolation, // 23502
        ForeignKeyViolation, // 23503
        Deadlock, // 40P01
        SerializationFailure, // 40001
        PrivilegeError, // 42501 / 28***
        DataException, // 22***
        TransactionState, // 25*** / 40*** rollback
        InternalError, // XX***
        Other // fallback
    };

    inline const char* toString(PgErrorCode code) noexcept
    {
        switch (code)
        {
        case PgErrorCode::OK: return "OK";
        case PgErrorCode::InvalidFuture: return "InvalidFuture";
        case PgErrorCode::ConnectionClosed: return "ConnectionClosed";
        case PgErrorCode::SocketReadFailed: return "SocketReadFailed";
        case PgErrorCode::ProtocolCorrupt: return "ProtocolCorrupt";
        case PgErrorCode::ParserTruncatedField: return "ParserTruncatedField";
        case PgErrorCode::ParserTruncatedRow: return "ParserTruncatedRow";
        case PgErrorCode::ParserTruncatedHeader: return "ParserTruncatedHeader";
        case PgErrorCode::ServerError: return "ServerError";
        case PgErrorCode::AuthFailed: return "AuthFailed";
        case PgErrorCode::AwaitCanceled: return "AwaitCanceled";
        case PgErrorCode::Unknown: return "Unknown";
        }
        return "InvalidPgErrorCode";
    }

    inline const char* toString(PgSqlStateClass cls) noexcept
    {
        switch (cls)
        {
        case PgSqlStateClass::None: return "None";
        case PgSqlStateClass::ConnectionError: return "ConnectionError";
        case PgSqlStateClass::SyntaxError: return "SyntaxError";
        case PgSqlStateClass::UndefinedObject: return "UndefinedObject";
        case PgSqlStateClass::ConstraintViolation: return "ConstraintViolation";
        case PgSqlStateClass::UniqueViolation: return "UniqueViolation";
        case PgSqlStateClass::CheckViolation: return "CheckViolation";
        case PgSqlStateClass::NotNullViolation: return "NotNullViolation";
        case PgSqlStateClass::ForeignKeyViolation: return "ForeignKeyViolation";
        case PgSqlStateClass::Deadlock: return "Deadlock";
        case PgSqlStateClass::SerializationFailure: return "SerializationFailure";
        case PgSqlStateClass::PrivilegeError: return "PrivilegeError";
        case PgSqlStateClass::DataException: return "DataException";
        case PgSqlStateClass::TransactionState: return "TransactionState";
        case PgSqlStateClass::InternalError: return "InternalError";
        case PgSqlStateClass::Other: return "Other";
        }
        return "InvalidPgSqlStateClass";
    }

    struct PgErrorDetail
    {
        std::string sqlstate;
        std::string message;
        std::string detail;
        std::string hint;
        PgSqlStateClass category{PgSqlStateClass::None};
    };

    struct QueryResult
    {
        struct Row
        {
            std::vector<std::string> cols;

            const std::string& operator[](size_t i) const noexcept
            {
                return cols[i];
            }

            std::string& operator[](size_t i) noexcept
            {
                return cols[i];
            }

            [[nodiscard]] inline size_t size() const noexcept { return this->cols.size(); }
            [[nodiscard]] inline bool empty() const noexcept { return this->cols.empty(); }
        };

        std::vector<Row> rows;

        const Row& operator[](size_t i) const noexcept
        {
            return this->rows[i];
        }

        Row& operator[](size_t i) noexcept
        {
            return this->rows[i];
        }

        bool ok{false};
        PgErrorCode code{PgErrorCode::Unknown};

        std::string error;

        PgErrorDetail err_detail;

        bool rows_valid{true};

        [[nodiscard]] inline bool empty() const noexcept { return this->ok && this->rows_valid && this->rows.empty(); }
        [[nodiscard]] inline bool has_rows() const noexcept { return this->ok && this->rows_valid && !this->rows.empty(); }
        [[nodiscard]] inline size_t row_count() const noexcept { return this->rows.size(); }
        [[nodiscard]] inline size_t col_count() const noexcept { return this->rows.empty() ? 0 : this->rows[0].cols.size(); }

        [[nodiscard]] inline bool invariant() const noexcept { return this->rows.empty() || !this->rows[0].cols.empty(); }
    };

    class QueryState : public utils::sync::refc::RefCounted<QueryState>
    {
    public:
        std::atomic<bool> ready{false};
        QueryResult result;

        std::string sql;

        std::coroutine_handle<> awaiting_coro{nullptr};

        std::atomic<bool> canceled{false};
        PgErrorCode cancel_code{PgErrorCode::OK};
        std::string cancel_reason;
    };

    class QueryAwaiter
    {
    public:
        explicit QueryAwaiter(std::shared_ptr<QueryState> st)
            : state(std::move(st))
        {
        }

        bool await_ready() const noexcept
        {
            return this->state->ready.load(std::memory_order_acquire);
        }

        void await_suspend(std::coroutine_handle<> h) noexcept
        {
            this->state->awaiting_coro = h;
        }

        QueryResult await_resume() noexcept
        {
            return this->state->result;
        }

    private:
        std::shared_ptr<QueryState> state;
    };

    class QueryFuture
    {
    public:
        QueryFuture() = default;

        explicit QueryFuture(std::shared_ptr<QueryState> st)
            : state(std::move(st))
        {
        }

        QueryAwaiter operator co_await() const noexcept
        {
            return QueryAwaiter(this->state);
        }

        QueryResult wait();

        bool valid() const noexcept { return (bool)this->state; }

        std::shared_ptr<QueryState> raw() const noexcept { return this->state; }

    private:
        std::shared_ptr<QueryState> state;
    };

    void write_be32(uint8_t* dst, uint32_t v);

    uint32_t read_be32(const uint8_t* src);

    struct PgFrame
    {
        char type;
        std::vector<uint8_t> payload;
    };

    struct PgWireError
    {
        PgErrorCode code{PgErrorCode::Unknown};
        std::string message;
    };

    template <class T>
    struct PgWireResult
    {
        T value{};
        bool ok{false};
        PgWireError err;
    };

    template <>
    struct PgWireResult<void>
    {
        bool ok{false};
        PgWireError err;
    };

    // classify SQLSTATE string ("23505", "40P01", etc.) into PgSqlStateClass
    PgSqlStateClass classify_sqlstate(std::string_view sqlstate);

    template <class Socket>
    uvent::task::Awaitable<PgWireResult<void>>
    read_exact(Socket& sock, std::vector<uint8_t>& buf, size_t n)
    {
        PgWireResult<void> out;
        out.ok = false;
        out.err.code = PgErrorCode::Unknown;
        out.err.message.clear();

        buf.resize(n);
        size_t off = 0;

        while (off < n)
        {
            uvent::utils::DynamicBuffer tmp;
            tmp.reserve(n - off);

            ssize_t r = co_await sock.async_read(tmp, n - off);
            if (r <= 0)
            {
                out.err.code = PgErrorCode::SocketReadFailed;
                out.err.message = "async_read returned <=0";
                co_return out;
            }

            std::memcpy(buf.data() + off, tmp.data(), (size_t)r);
            off += (size_t)r;
        }

        out.ok = true;
        co_return out;
    }

    template <class Socket>
    uvent::task::Awaitable<PgWireResult<PgFrame>>
    read_frame(Socket& sock)
    {
        PgWireResult<PgFrame> out;
        out.ok = false;
        out.err.code = PgErrorCode::Unknown;
        out.err.message.clear();

        std::vector<uint8_t> header;
        {
            auto hdr_res = co_await read_exact(sock, header, 5);
            if (!hdr_res.ok)
            {
                out.err = hdr_res.err;
                co_return out;
            }
        }

        char type = (char)header[0];
        uint32_t len = read_be32(&header[1]);

        if (len < 4)
        {
            out.err.code = PgErrorCode::ProtocolCorrupt;
            out.err.message = "frame length < 4";
            co_return out;
        }

        uint32_t payload_len = len - 4;

        std::vector<uint8_t> payload;
        {
            auto pay_res = co_await read_exact(sock, payload, payload_len);
            if (!pay_res.ok)
            {
                out.err = pay_res.err;
                co_return out;
            }
        }

        out.value.type = type;
        out.value.payload = std::move(payload);
        out.ok = true;
        co_return out;
    }

    struct PgServerErrorFields
    {
        std::string severity;
        std::string sqlstate;
        std::string message;
        std::string detail;
        std::string hint;
    };

    PgServerErrorFields parse_error_fields(const std::vector<uint8_t>& payload);

    std::string parse_error(const std::vector<uint8_t>& payload);

    struct RowParseContext
    {
        bool ok{true};
        PgErrorCode code{PgErrorCode::OK};
        std::string msg;
    };

    void parse_row_description_ex(const std::vector<uint8_t>& payload,
                                  std::vector<std::string>& out_cols,
                                  RowParseContext& ctx);

    inline void parse_row_description(const std::vector<uint8_t>& payload,
                                      std::vector<std::string>& out_cols)
    {
        RowParseContext ctx;
        parse_row_description_ex(payload, out_cols, ctx);
    }

    void parse_data_row_ex(const std::vector<uint8_t>& payload,
                           QueryResult::Row& out_row,
                           RowParseContext& ctx);

    inline void parse_data_row(const std::vector<uint8_t>& payload,
                               QueryResult::Row& out_row)
    {
        RowParseContext ctx;
        parse_data_row_ex(payload, out_row, ctx);
    }

    std::vector<uint8_t> build_startup_message(const std::string& user,
                                               const std::string& db);

    std::vector<uint8_t> build_password_message(const std::string& password);

    std::string md5_hex(const uint8_t* data, size_t len);

    std::vector<uint8_t> build_md5_password_message(
        const std::string& user,
        const std::string& password,
        const uint8_t salt[4]
    );

    void build_simple_query(std::vector<uint8_t>& out,
                            std::string_view sql);

    struct PgCopyResult
    {
        bool ok{false};
        PgErrorCode code{PgErrorCode::Unknown};
        std::string error;
        PgErrorDetail err_detail;
        uint64_t rows_affected{0};
    };

    struct PgCursorChunk
    {
        std::vector<QueryResult::Row> rows;
        bool done{false};
        bool ok{false};
        PgErrorCode code{PgErrorCode::Unknown};
        std::string error;
        PgErrorDetail err_detail;
    };
} // namespace usub::pg

#endif
