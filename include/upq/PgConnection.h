#ifndef PGCONNECTIONLIBPQ_H
#define PGCONNECTIONLIBPQ_H

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <chrono>
#include <type_traits>
#include <charconv>
#include <bit>
#include <array>
#include <cstring>

#include <libpq-fe.h>

#include "uvent/Uvent.h"
#include "PgTypes.h"
#include "meta/PgConcepts.h"

namespace usub::pg
{
    namespace detail
    {
        constexpr Oid BOOLOID = 16;
        constexpr Oid INT2OID = 21;
        constexpr Oid INT4OID = 23;
        constexpr Oid INT8OID = 20;
        constexpr Oid FLOAT4OID = 700;
        constexpr Oid FLOAT8OID = 701;
        constexpr Oid TEXTOID = 25;

        template <class T>
        concept StringLike = std::is_same_v<std::decay_t<T>, std::string> ||
            std::is_same_v<std::decay_t<T>, std::string_view>;

        template <class T>
        concept CharPtr = std::is_same_v<std::decay_t<T>, const char*> ||
            std::is_same_v<std::decay_t<T>, char*>;

        template <class T>
        concept Integral = std::is_integral_v<std::decay_t<T>> &&
            !std::is_same_v<std::decay_t<T>, bool>;

        template <class T>
        concept Floating = std::is_floating_point_v<std::decay_t<T>>;

        template <class T>
        concept Optional = requires { typename std::decay_t<T>::value_type; } &&
            std::is_same_v<std::decay_t<T>, std::optional<typename std::decay_t<T>::value_type>>;

        template <class T>
        inline T to_be(T v) requires std::is_integral_v<T>
        {
            if constexpr (std::endian::native == std::endian::big) return v;
#if __cpp_lib_byteswap >= 202110L
            return std::byteswap(v);
#else
            if constexpr (sizeof(T) == 2) {
                uint16_t x = static_cast<uint16_t>(v);
                x = (uint16_t)((x << 8) | (x >> 8));
                return static_cast<T>(x);
            } else if constexpr (sizeof(T) == 4) {
                uint32_t x = static_cast<uint32_t>(v);
                x = (x << 24) | ((x & 0x0000FF00u) << 8) | ((x & 0x00FF0000u) >> 8) | (x >> 24);
                return static_cast<T>(x);
            } else { // 8
                uint64_t x = static_cast<uint64_t>(v);
                x = (x << 56) |
                    ((x & 0x000000000000FF00ull) << 40) |
                    ((x & 0x0000000000FF0000ull) << 24) |
                    ((x & 0x00000000FF000000ull) << 8)  |
                    ((x & 0x000000FF00000000ull) >> 8)  |
                    ((x & 0x0000FF0000000000ull) >> 24) |
                    ((x & 0x00FF000000000000ull) >> 40) |
                    (x >> 56);
                return static_cast<T>(x);
            }
#endif
        }

        inline uint32_t f32_be(float v) { return to_be(std::bit_cast<uint32_t>(v)); }
        inline uint64_t f64_be(double v) { return to_be(std::bit_cast<uint64_t>(v)); }

        struct ParamSlices
        {
            const char** values;
            int* lengths;
            int* formats;
            Oid* types;
            size_t* idx;

            std::vector<std::string>& temp_strings;
            std::vector<std::vector<char>>& temp_bytes;

            inline void set_null()
            {
                this->values[*this->idx] = nullptr;
                this->lengths[*this->idx] = 0;
                this->formats[*this->idx] = 0;
                this->types[*this->idx] = 0;
                ++(*this->idx);
            }

            inline void set_text(std::string_view sv, Oid oid_hint = 0)
            {
                this->temp_strings.emplace_back(sv.data(), sv.size());
                this->values[*this->idx] = this->temp_strings.back().c_str();
                this->lengths[*this->idx] = 0;
                this->formats[*this->idx] = 0;
                this->types[*this->idx] = oid_hint;
                ++(*this->idx);
            }

            template <class T>
            inline void set_bin_integral(T v, Oid oid)
            {
                T be = to_be(v);
                auto& vec = this->temp_bytes.emplace_back();
                vec.resize(sizeof(T));
                std::memcpy(vec.data(), &be, sizeof(T));
                this->values[*this->idx] = vec.data();
                this->lengths[*this->idx] = (int)sizeof(T);
                this->formats[*this->idx] = 1;
                this->types[*this->idx] = oid;
                ++(*this->idx);
            }

            inline void set_bin_bool(bool b)
            {
                auto& vec = this->temp_bytes.emplace_back();
                vec.resize(1);
                vec[0] = static_cast<char>(b ? 1 : 0);
                this->values[*this->idx] = vec.data();
                this->lengths[*this->idx] = 1;
                this->formats[*this->idx] = 1;
                this->types[*this->idx] = BOOLOID;
                ++(*this->idx);
            }

            inline void set_bin_f32(float v)
            {
                uint32_t be = f32_be(v);
                auto& vec = this->temp_bytes.emplace_back();
                vec.resize(4);
                std::memcpy(vec.data(), &be, 4);
                this->values[*this->idx] = vec.data();
                this->lengths[*this->idx] = 4;
                this->formats[*this->idx] = 1;
                this->types[*this->idx] = FLOAT4OID;
                ++(*this->idx);
            }

            inline void set_bin_f64(double v)
            {
                uint64_t be = f64_be(v);
                auto& vec = this->temp_bytes.emplace_back();
                vec.resize(8);
                std::memcpy(vec.data(), &be, 8);
                this->values[*this->idx] = vec.data();
                this->lengths[*this->idx] = 8;
                this->formats[*this->idx] = 1;
                this->types[*this->idx] = FLOAT8OID;
                ++(*this->idx);
            }
        };

        template <class Int>
        inline void set_text_int(ParamSlices& ps, Int v)
        {
            std::array<char, 32> buf{};
            auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), v);
            std::string_view sv(buf.data(), static_cast<size_t>(ptr - buf.data()));
            ps.set_text(sv, 0);
        }

        template <class T>
        inline void encode_one(ParamSlices& ps, T&& v)
        {
            using U = std::decay_t<T>;
            if constexpr (std::is_same_v<U, bool>)
            {
                ps.set_bin_bool(v);
            }
            else if constexpr (Integral<U>)
            {
                if constexpr (sizeof(U) == 1)
                {
                    ps.set_bin_integral<int16_t>(static_cast<int16_t>(v), INT2OID);
                }
                else if constexpr (sizeof(U) == 2)
                {
                    ps.set_bin_integral<int16_t>(static_cast<int16_t>(v), INT2OID);
                }
                else if constexpr (sizeof(U) == 4)
                {
                    ps.set_bin_integral<int32_t>(static_cast<int32_t>(v), INT4OID);
                }
                else
                {
                    // 8
                    ps.set_bin_integral<int64_t>(static_cast<int64_t>(v), INT8OID);
                }
            }
            else if constexpr (Floating<U>)
            {
                if constexpr (std::is_same_v<U, float>) ps.set_bin_f32(v);
                else ps.set_bin_f64(static_cast<double>(v));
            }
            else if constexpr (StringLike<U>)
            {
                ps.set_text(std::string_view(v));
            }
            else if constexpr (CharPtr<U>)
            {
                if (!v) ps.set_null();
                else ps.set_text(std::string_view(v));
            }
            else
            {
                if constexpr (Integral<U>)
                {
                    set_text_int(ps, v);
                }
                else
                {
                    std::string s = std::string(v);
                    ps.set_text(s);
                }
            }
        }

        template <Optional Opt>
        inline void encode_one(ParamSlices& ps, Opt&& ov)
        {
            using T = std::decay_t<typename std::decay_t<Opt>::value_type>;
            if (!ov)
            {
                ps.set_null();
                return;
            }
            encode_one(ps, *ov);
        }
    } // namespace detail

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

    // ===== error helpers =====
    inline void fill_server_error_fields(PGresult* res, QueryResult& out)
    {
        if (!res) return;

        const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        const char* primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char* detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        const char* hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);

        if (primary && *primary) out.error = primary;
        else if (const char* fb = PQresultErrorMessage(res); fb && *fb) out.error = fb;

        if (sqlstate) out.err_detail.sqlstate = sqlstate;
        if (detail) out.err_detail.detail = detail;
        if (hint) out.err_detail.hint = hint;

        if (primary && *primary) out.err_detail.message = primary;
        else if (!out.error.empty()) out.err_detail.message = out.error;

        out.err_detail.category = classify_sqlstate(out.err_detail.sqlstate);

        out.ok = false;
        out.code = PgErrorCode::ServerError;
        out.rows_valid = false;
    }

    inline void fill_server_error_fields_copy(PGresult* res, PgCopyResult& out)
    {
        if (!res) return;

        const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        const char* primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char* detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        const char* hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);

        if (primary && *primary) out.error = primary;
        else if (const char* fb = PQresultErrorMessage(res); fb && *fb) out.error = fb;

        if (sqlstate) out.err_detail.sqlstate = sqlstate;
        if (detail) out.err_detail.detail = detail;
        if (hint) out.err_detail.hint = hint;

        if (primary && *primary) out.err_detail.message = primary;
        else if (!out.error.empty()) out.err_detail.message = out.error;

        out.err_detail.category = classify_sqlstate(out.err_detail.sqlstate);

        out.ok = false;
        out.code = PgErrorCode::ServerError;
    }

    inline void fill_server_error_fields_cursor(PGresult* res, PgCursorChunk& out)
    {
        if (!res) return;

        const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        const char* primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char* detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        const char* hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);

        if (primary && *primary) out.error = primary;
        else if (const char* fb = PQresultErrorMessage(res); fb && *fb) out.error = fb;

        if (sqlstate) out.err_detail.sqlstate = sqlstate;
        if (detail) out.err_detail.detail = detail;
        if (hint) out.err_detail.hint = hint;

        if (primary && *primary) out.err_detail.message = primary;
        else if (!out.error.empty()) out.err_detail.message = out.error;

        out.err_detail.category = classify_sqlstate(out.err_detail.sqlstate);

        out.ok = false;
        out.code = PgErrorCode::ServerError;
    }

    // ===== exec_param_query_nonblocking =====
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

        std::vector<std::string> temp_strings;
        std::vector<std::vector<char>> temp_bytes;
        temp_strings.reserve(N);
        temp_bytes.reserve(N);

        size_t idx = 0;
        detail::ParamSlices ps{values, lengths, formats, types, &idx, temp_strings, temp_bytes};

        (detail::encode_one(ps, std::forward<Args>(args)), ...);

        if (!PQsendQueryParams(this->conn_,
                               sql.c_str(),
                               static_cast<int>(N),
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
            if (fr == 0) break;
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
                                row.cols.emplace_back();
                            else
                            {
                                const char* v = PQgetvalue(res, r, c);
                                int len = PQgetlength(res, r, c);
                                row.cols.emplace_back(v, static_cast<size_t>(len));
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
