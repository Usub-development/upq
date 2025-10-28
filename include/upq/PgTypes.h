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

    struct QueryResult
    {
        struct Row
        {
            std::vector<std::string> cols;
        };

        std::vector<Row> rows;
        bool ok{false};
        PgErrorCode code{PgErrorCode::Unknown};
        std::string error;
        std::string server_sqlstate;
        std::string server_detail;
        std::string server_hint;
        bool rows_valid{true};
    };

    class QueryState : public utils::sync::refc::RefCounted<QueryState>
    {
    public:
        std::atomic<bool> ready{false};
        QueryResult result;

        std::string sql;

        std::mutex mtx;
        std::condition_variable cv;

        std::coroutine_handle<> awaiting_coro{nullptr};

        std::atomic<bool> canceled{false};
        PgErrorCode cancel_code{PgErrorCode::OK};
        std::string cancel_reason;

        void set_result(QueryResult&& r);

        void set_canceled(PgErrorCode code, std::string msg);
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
} // namespace usub::pg

#endif // PGTYPES_H