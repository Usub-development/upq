#ifndef PGCONNECTIONLIBPQ_H
#define PGCONNECTIONLIBPQ_H

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <chrono>
#include <type_traits>
#include <typeinfo>
#include <sstream>
#include <bit>        // std::endian, std::bit_cast
#include <utility>    // std::byteswap
#include <cstdint>
#include <cstring>
#include <limits>

#include <libpq-fe.h>

#include "uvent/Uvent.h"
#include "PgTypes.h"
#include "meta/PgConcepts.h"

namespace usub::pg
{
    // ---------- server-error mappers ----------
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

    // ---------- endian helpers ----------
    constexpr uint16_t to_be16(uint16_t v) noexcept
    {
        if constexpr (std::endian::native == std::endian::little) return std::byteswap(v);
        else return v;
    }

    constexpr uint32_t to_be32(uint32_t v) noexcept
    {
        if constexpr (std::endian::native == std::endian::little) return std::byteswap(v);
        else return v;
    }

    constexpr uint64_t to_be64(uint64_t v) noexcept
    {
        if constexpr (std::endian::native == std::endian::little) return std::byteswap(v);
        else return v;
    }

    inline uint32_t fp_to_be(float v) noexcept
    {
        static_assert(std::numeric_limits<float>::is_iec559);
        return to_be32(std::bit_cast<uint32_t>(v));
    }

    inline uint64_t fp_to_be(double v) noexcept
    {
        static_assert(std::numeric_limits<double>::is_iec559);
        return to_be64(std::bit_cast<uint64_t>(v));
    }

    // ---------- detail: OIDs + concepts ----------
    namespace detail
    {
        // OIDs (pg_type.h)
        constexpr Oid BOOLOID = 16;
        constexpr Oid INT8OID = 20;
        constexpr Oid INT2OID = 21;
        constexpr Oid INT4OID = 23;
        constexpr Oid TEXTOID = 25;
        constexpr Oid FLOAT4OID = 700;
        constexpr Oid FLOAT8OID = 701;

        template <class T>
        using Decay = std::decay_t<T>;
        template <class T> concept StringLike = std::is_same_v<Decay<T>, std::string> || std::is_same_v<
            Decay<T>, std::string_view>;
        template <class T> concept CharPtr = std::is_same_v<Decay<T>, const char*> || std::is_same_v<Decay<T>, char*>;
        template <class T> concept Integral = std::is_integral_v<Decay<T>> && !std::is_same_v<Decay<T>, bool>;
        template <class T> concept Floating = std::is_floating_point_v<Decay<T>>;
        template <class T> concept Optional = requires { typename Decay<T>::value_type; }
            && std::is_same_v<Decay<T>, std::optional<typename Decay<T>::value_type>>;
        template <class T> concept Streamable = requires(std::ostream& os, const T& v)
        {
            { os << v } -> std::same_as<std::ostream&>;
        };
    } // namespace detail

    // ---------- param encoder buffers ----------
    struct ParamSlices
    {
        const char** values;
        int* lengths;
        int* formats;
        Oid* types;
        size_t* idx;
        std::vector<std::string>& temp_strings;
        std::vector<std::vector<char>>& temp_bytes;

        void set_null()
        {
            const auto i = (*idx)++;
            values[i] = nullptr;
            lengths[i] = 0;
            formats[i] = 0;
            types[i] = 0;
        }

        void set_text(std::string_view sv)
        {
            const auto i = (*idx)++;
            temp_strings.emplace_back(sv);
            values[i] = temp_strings.back().c_str();
            lengths[i] = (int)sv.size();
            formats[i] = 0;
            types[i] = 0; // TEXT inferred
        }

        template <class BinT>
        void set_bin_raw(const void* data, size_t n, Oid oid)
        {
            const auto i = (*idx)++;
            temp_bytes.emplace_back();
            auto& buf = temp_bytes.back();
            buf.resize(n);
            std::memcpy(buf.data(), data, n);
            values[i] = buf.data();
            lengths[i] = (int)n;
            formats[i] = 1; // binary
            types[i] = oid;
        }

        template <class IntT>
        void set_bin_integral(IntT v, Oid oid)
        {
            if constexpr (sizeof(IntT) == 2)
            {
                uint16_t be = to_be16((uint16_t)v);
                set_bin_raw<IntT>(&be, 2, oid);
            }
            else if constexpr (sizeof(IntT) == 4)
            {
                uint32_t be = to_be32((uint32_t)v);
                set_bin_raw<IntT>(&be, 4, oid);
            }
            else
            {
                uint64_t be = to_be64((uint64_t)v);
                set_bin_raw<IntT>(&be, 8, oid);
            }
        }

        void set_bin_f32(float v)
        {
            uint32_t be = fp_to_be(v);
            set_bin_raw<float>(&be, 4, detail::FLOAT4OID);
        }

        void set_bin_f64(double v)
        {
            uint64_t be = fp_to_be(v);
            set_bin_raw<double>(&be, 8, detail::FLOAT8OID);
        }

        void set_bin_bool(bool v)
        {
            unsigned char b = v ? 1u : 0u;
            set_bin_raw<bool>(&b, 1, detail::BOOLOID);
        }
    };

    // ---------- encoding dispatch ----------
    namespace detail
    {
        inline void encode_one(ParamSlices& ps, bool v) { ps.set_bin_bool(v); }

        template <Integral T>
        inline void encode_one(ParamSlices& ps, T v)
        {
            if constexpr (sizeof(T) <= 2) ps.set_bin_integral<int16_t>(static_cast<int16_t>(v), detail::INT2OID);
            else if constexpr (sizeof(T) == 4) ps.set_bin_integral<int32_t>(static_cast<int32_t>(v), detail::INT4OID);
            else ps.set_bin_integral<int64_t>(static_cast<int64_t>(v), detail::INT8OID);
        }

        inline void encode_one(ParamSlices& ps, float v) { ps.set_bin_f32(v); }
        inline void encode_one(ParamSlices& ps, double v) { ps.set_bin_f64(v); }
        inline void encode_one(ParamSlices& ps, std::string_view v) { ps.set_text(v); }
        inline void encode_one(ParamSlices& ps, const std::string& v) { ps.set_text(v); }

        template <size_t N>
        inline void encode_one(ParamSlices& ps, const char (&lit)[N])
        {
            ps.set_text(std::string_view(lit, N ? (N - 1) : 0));
        }

        inline void encode_one(ParamSlices& ps, const char* v)
        {
            if (v) ps.set_text(v);
            else ps.set_null();
        }

        template <Optional Opt>
        inline void encode_one(ParamSlices& ps, Opt&& ov)
        {
            if (!ov) ps.set_null();
            else encode_one(ps, *ov);
        }

        template <class T>
            requires (!Integral<T> && !Floating<T> && !StringLike<T> && !CharPtr<T> &&
                !Optional<T> && !std::is_same_v<Decay<T>, bool>)
        inline void encode_one(ParamSlices& ps, T&& v)
        {
            if constexpr (std::is_convertible_v<T, std::string_view>)
            {
                ps.set_text(std::string_view(v));
            }
            else if constexpr (std::is_constructible_v<std::string, T>)
            {
                ps.set_text(std::string(std::forward<T>(v)));
            }
            else if constexpr (Streamable<T>)
            {
                std::ostringstream oss;
                oss << v;
                ps.set_text(oss.str());
            }
            else
            {
                std::ostringstream oss;
                oss << "<param:" << typeid(Decay<T>).name() << ">";
                ps.set_text(oss.str());
            }
        }
    } // namespace detail

    // ---------- class ----------
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

        template <bool Pipeline = false, typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        exec_param_query_nonblocking(const std::string& sql, Args&&... args);

        usub::uvent::task::Awaitable<PgCopyResult> copy_in_start(const std::string& sql);
        usub::uvent::task::Awaitable<PgCopyResult> copy_in_send_chunk(const void* data, size_t len);
        usub::uvent::task::Awaitable<PgCopyResult> copy_in_finish();
        usub::uvent::task::Awaitable<PgCopyResult> copy_out_start(const std::string& sql);
        usub::uvent::task::Awaitable<PgWireResult<std::vector<uint8_t>>> copy_out_read_chunk();

        std::string make_cursor_name();
        usub::uvent::task::Awaitable<QueryResult>
        cursor_declare(const std::string& cursor_name, const std::string& sql);
        usub::uvent::task::Awaitable<PgCursorChunk> cursor_fetch_chunk(const std::string& cursor_name, uint32_t count);
        usub::uvent::task::Awaitable<QueryResult> cursor_close(const std::string& cursor_name);

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


    // ---------- impl: exec_param_query_nonblocking ----------
    template <bool Pipeline, typename... Args>
    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::exec_param_query_nonblocking(const std::string& sql, Args&&... args)
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

        bool entered = false;
        if constexpr (Pipeline)
        {
            if (PQpipelineStatus(conn_) != PQ_PIPELINE_ON)
                entered = (PQenterPipelineMode(conn_) == 1);
        }

        constexpr size_t N = sizeof...(Args);
        const char* values[N];
        int lengths[N];
        int formats[N];
        Oid types[N];
        std::vector<std::string> temp_strings;
        temp_strings.reserve(N);
        std::vector<std::vector<char>> temp_bytes;
        temp_bytes.reserve(N);

        size_t idx = 0;
        ParamSlices ps{values, lengths, formats, types, &idx, temp_strings, temp_bytes};
        (detail::encode_one(ps, std::forward<Args>(args)), ...);

        if (!PQsendQueryParams(conn_, sql.c_str(), static_cast<int>(N),
                               types, values, lengths, formats, /*resultFormat=*/0))
        {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            out.rows_valid = false;
            if constexpr (Pipeline) { if (entered) PQexitPipelineMode(conn_); }
            co_return out;
        }

        // flush
        for (;;)
        {
            const int fr = PQflush(conn_);
            if (fr == 0) break;
            if (fr == -1)
            {
                out.ok = false;
                out.code = PgErrorCode::SocketReadFailed;
                out.error = PQerrorMessage(conn_);
                out.rows_valid = false;
                if constexpr (Pipeline) { if (entered) PQexitPipelineMode(conn_); }
                co_return out;
            }
            co_await wait_writable();
        }

        // read: consume → drain results → if busy await, else finish
        for (;;)
        {
            if (PQconsumeInput(conn_) == 0)
            {
                out.ok = false;
                out.code = PgErrorCode::SocketReadFailed;
                out.error = PQerrorMessage(conn_);
                out.rows_valid = false;
                if constexpr (Pipeline) { if (entered) PQexitPipelineMode(conn_); }
                co_return out;
            }

            bool saw_any = false;
            while (PGresult* res = PQgetResult(conn_))
            {
                saw_any = true;
                const auto st = PQresultStatus(res);
                if (st == PGRES_TUPLES_OK)
                {
                    const int nrows = PQntuples(res);
                    const int ncols = PQnfields(res);
                    if (nrows > 0) out.rows.reserve(out.rows.size() + nrows);

                    for (int r = 0; r < nrows; ++r)
                    {
                        QueryResult::Row row;
                        row.cols.reserve(ncols);
                        for (int c = 0; c < ncols; ++c)
                        {
                            if (PQgetisnull(res, r, c)) row.cols.emplace_back();
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
                PQclear(res);
            }

            if (!PQisBusy(conn_))
            {
                if (saw_any && out.error.empty())
                {
                    out.ok = true;
                    out.code = PgErrorCode::OK;
                    out.rows_valid = true;
                }
                if constexpr (Pipeline) { if (entered) PQexitPipelineMode(conn_); }
                co_return out;
            }

            co_await wait_readable();
        }
    }
} // namespace usub::pg

#endif // PGCONNECTIONLIBPQ_H
