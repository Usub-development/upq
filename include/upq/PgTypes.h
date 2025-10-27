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
    struct QueryResult
    {
        struct Row
        {
            std::vector<std::string> cols;
        };

        std::vector<Row> rows;
        bool ok{false};
        std::string error;
    };

    class QueryState : public usub::utils::sync::refc::RefCounted<QueryState>
    {
    public:
        std::atomic<bool> ready{false};
        QueryResult result;

        std::string sql;

        std::mutex mtx;
        std::condition_variable cv;

        std::coroutine_handle<> awaiting_coro{nullptr};

        void set_result(QueryResult&& r);
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

    template <class Socket>
    usub::uvent::task::Awaitable<bool>
    read_exact(Socket& sock, std::vector<uint8_t>& buf, size_t n)
    {
        buf.resize(n);
        size_t off = 0;

        while (off < n)
        {
            usub::uvent::utils::DynamicBuffer tmp;
            tmp.reserve(n - off);

            ssize_t r = co_await sock.async_read(tmp, n - off);

            if (r <= 0)
            {
                co_return false;
            }

            std::memcpy(buf.data() + off, tmp.data(), (size_t)r);
            off += (size_t)r;
        }

        co_return true;
    }

    template <class Socket>
    usub::uvent::task::Awaitable<std::optional<PgFrame>>
    read_frame(Socket& sock)
    {
        std::vector<uint8_t> header;
        bool ok_hdr = co_await read_exact(sock, header, 5);
        if (!ok_hdr)
        {
            co_return std::nullopt;
        }

        char type = (char)header[0];
        uint32_t len = read_be32(&header[1]);

        if (len < 4)
        {
            co_return std::nullopt;
        }

        uint32_t payload_len = len - 4;

        std::vector<uint8_t> payload;
        bool ok_payload = co_await read_exact(sock, payload, payload_len);
        if (!ok_payload)
        {
            co_return std::nullopt;
        }

        PgFrame frame{
            .type = type,
            .payload = std::move(payload),
        };

        co_return frame;
    }

    std::string parse_error(const std::vector<uint8_t>& payload);

    void parse_row_description(const std::vector<uint8_t>& payload,
                               std::vector<std::string>& out_cols);

    void parse_data_row(const std::vector<uint8_t>& payload,
                        QueryResult::Row& out_row);

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
