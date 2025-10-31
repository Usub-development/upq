#ifndef PGCONNECTIONLIBPQ_H
#define PGCONNECTIONLIBPQ_H

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <array>
#include <list>
#include <initializer_list>
#include <chrono>
#include <type_traits>
#include <typeinfo>
#include <sstream>
#include <bit>        // std::endian, std::bit_cast
#include <utility>    // std::byteswap
#include <cstdint>
#include <cstring>
#include <limits>
#include <iterator>

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

        // array OIDs
        constexpr Oid BOOLARRAYOID = 1000;
        constexpr Oid INT2ARRAYOID = 1005;
        constexpr Oid INT4ARRAYOID = 1007;
        constexpr Oid TEXTARRAYOID = 1009;
        constexpr Oid INT8ARRAYOID = 1016;
        constexpr Oid FLOAT4ARRAYOID = 1021;
        constexpr Oid FLOAT8ARRAYOID = 1022;

        template <class T>
        using Decay = std::decay_t<T>;

        // scalar concepts
        template <class T> concept StringLike =
            std::is_same_v<Decay<T>, std::string> ||
            std::is_same_v<Decay<T>, std::string_view>;

        template <class T> concept CharPtr =
            std::is_same_v<Decay<T>, const char*> || std::is_same_v<Decay<T>, char*>;

        template <class T> concept Integral = std::is_integral_v<Decay<T>> && !std::is_same_v<Decay<T>, bool>;
        template <class T> concept Floating = std::is_floating_point_v<Decay<T>>;

        template <class T> concept Optional =
            requires { typename Decay<T>::value_type; } &&
            std::is_same_v<Decay<T>, std::optional<typename Decay<T>::value_type>>;

        template <class T>
        concept Streamable = requires(std::ostream& os, const T& v) { { os << v } -> std::same_as<std::ostream&>; };

        // container detectors
        template <class, class = void>
        struct has_mapped_type : std::false_type
        {
        };

        template <class T>
        struct has_mapped_type<T, std::void_t<typename T::mapped_type>> : std::true_type
        {
        };

        template <class T>
        inline constexpr bool has_mapped_type_v = has_mapped_type<T>::value;

        template <class T>
        concept AssociativeLike = has_mapped_type_v<Decay<T>> || (requires { typename Decay<T>::key_type; });

        template <class T>
        concept HasBeginEnd = requires(T& t)
        {
            { std::begin(t) } -> std::input_or_output_iterator;
            { std::end(t) } -> std::sentinel_for<decltype(std::begin(t))>;
        };

        template <class T>
        concept HasSize = requires(const T& t) { { t.size() } -> std::convertible_to<size_t>; };

        // init_list detector to exclude from ArrayLike
        template <class T>
        concept InitList =
            requires { typename Decay<T>::value_type; } &&
            std::is_same_v<Decay<T>, std::initializer_list<typename Decay<T>::value_type>>;

        template <class T>
        concept ArrayLike =
            HasBeginEnd<T> &&
            !StringLike<T> &&
            !CharPtr<T> &&
            !AssociativeLike<T> &&
            !InitList<T> &&
            requires { typename Decay<T>::value_type; };

        // FIX: detect C-arrays on the original type (not decay) to catch T(&)[N]
        template <class T>
        concept CArrayLike = std::is_array_v<std::remove_reference_t<T>>;

        // ---------- PG array literal helpers ----------
        inline void pg_array_escape_elem(std::string& out, std::string_view s)
        {
            out.push_back('"');
            for (char ch : s)
            {
                if (ch == '"' || ch == '\\') out.push_back('\\');
                out.push_back(ch);
            }
            out.push_back('"');
        }

        template <class Cnt>
        inline size_t guess_array_reserve(const Cnt& c)
        {
            if constexpr (HasSize<Cnt>) return 2 + c.size() * 8;
            else return 64;
        }

        template <class T>
        inline void write_array_scalar(std::string& out, const T& v)
        {
            if constexpr (Optional<T>)
            {
                if (!v)
                {
                    out += "NULL";
                    return;
                }
                write_array_scalar(out, *v);
            }
            else if constexpr (std::is_same_v<Decay<T>, bool>)
            {
                out += (v ? "t" : "f");
            }
            else if constexpr (Integral<T>)
            {
                if constexpr (std::is_same_v<Decay<T>, long long> || std::is_same_v<Decay<T>, unsigned long long>)
                    out += std::to_string(v);
                else
                    out += std::to_string(static_cast<long long>(v));
            }
            else if constexpr (Floating<T>)
            {
                out += std::to_string(static_cast<long double>(v));
            }
            else if constexpr (StringLike<T>)
            {
                pg_array_escape_elem(out, std::string_view(v));
            }
            else if constexpr (CharPtr<T>)
            {
                if (v) pg_array_escape_elem(out, std::string_view(v, std::char_traits<char>::length(v)));
                else out += "NULL";
            }
            else if constexpr (std::is_pointer_v<Decay<T>>)
            {
                static_assert(!std::is_pointer_v<Decay<T>>,
                              "Unsupported pointer element in array (only char* allowed)");
            }
            else if constexpr (Streamable<T>)
            {
                std::ostringstream oss;
                oss << v;
                pg_array_escape_elem(out, oss.str());
            }
            else
            {
                std::ostringstream oss;
                oss << "<elem:" << typeid(Decay<T>).name() << ">";
                pg_array_escape_elem(out, oss.str());
            }
        }

        template <class Range>
        inline std::string build_pg_array_from_range(const Range& r)
        {
            std::string buf;
            buf.reserve(guess_array_reserve(r));
            buf.push_back('{');
            bool first = true;
            for (auto&& e : r)
            {
                if (!first) buf.push_back(',');
                first = false;
                write_array_scalar(buf, e);
            }
            buf.push_back('}');
            return buf;
        }

        template <class T, size_t N>
        inline std::string build_pg_array_from_carray(const T (&a)[N])
        {
            std::string buf;
            buf.reserve(2 + N * 8);
            buf.push_back('{');
            for (size_t i = 0; i < N; ++i)
            {
                if (i) buf.push_back(',');
                write_array_scalar(buf, a[i]);
            }
            buf.push_back('}');
            return buf;
        }

        // выбор OID массива по типу элемента
        template <class Elem>
        consteval Oid pick_array_oid()
        {
            if constexpr (std::is_same_v<Decay<Elem>, bool>) return BOOLARRAYOID;
            else if constexpr (Integral<Elem>)
            {
                if constexpr (sizeof(Decay<Elem>) <= 2) return INT2ARRAYOID;
                else if constexpr (sizeof(Decay<Elem>) == 4) return INT4ARRAYOID;
                else return INT8ARRAYOID;
            }
            else if constexpr (std::is_same_v<Decay<Elem>, float>) return FLOAT4ARRAYOID;
            else if constexpr (std::is_same_v<Decay<Elem>, double>) return FLOAT8ARRAYOID;
            else return TEXTARRAYOID;
        }

        // снять optional
        template <class T>
        struct unopt
        {
            using type = T;
        };

        template <class U>
        struct unopt<std::optional<U>>
        {
            using type = U;
        };

        template <class T>
        using unopt_t = typename unopt<Decay<T>>::type;
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

        // текстовый параметр с явным типом (например, text[])
        void set_text_typed(std::string_view sv, Oid oid)
        {
            const auto i = (*idx)++;
            temp_strings.emplace_back(sv);
            values[i] = temp_strings.back().c_str();
            lengths[i] = (int)sv.size();
            formats[i] = 0; // text format
            types[i] = oid; // explicit type
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
        // --- scalars ---
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
            if (v) ps.set_text(std::string_view(v, std::char_traits<char>::length(v)));
            else ps.set_null();
        }

        template <Optional Opt>
        inline void encode_one(ParamSlices& ps, Opt&& ov)
        {
            if (!ov) ps.set_null();
            else encode_one(ps, *ov);
        }

        // ----- контейнеры → PG array (text literal с явным типом) -----
        template <ArrayLike C>
        inline void encode_one(ParamSlices& ps, const C& cont)
        {
            using Elem0 = typename C::value_type;
            using Elem = unopt_t<Elem0>;
            constexpr Oid arr_oid = pick_array_oid<Elem>();
            const std::string s = build_pg_array_from_range(cont);
            ps.set_text_typed(s, arr_oid);
        }

        // C-массив const T[N]
        template <class T, size_t N>
        inline void encode_one(ParamSlices& ps, const T (&arr)[N])
        {
            using Elem = unopt_t<T>;
            constexpr Oid arr_oid = pick_array_oid<Elem>();
            const std::string s = build_pg_array_from_carray(arr);
            ps.set_text_typed(s, arr_oid);
        }

        // C-массив T[N] (неконстантный) — форвард на const-версию
        template <class T, size_t N>
        inline void encode_one(ParamSlices& ps, T (&arr)[N])
        {
            encode_one(ps, const_cast<const T(&)[N]>(arr));
        }

        // --- initializer_list<T> как PG-массив (приоритетный оверлоад) ---
        template <class T>
        inline void encode_one(ParamSlices& ps, std::initializer_list<T> il)
        {
            using Elem = unopt_t<T>;
            constexpr Oid arr_oid = pick_array_oid<Elem>();
            const std::string s = build_pg_array_from_range(il);
            ps.set_text_typed(s, arr_oid);
        }

        // --- fallback ---
        template <class T>
            requires (!Integral<T> && !Floating<T> && !StringLike<T> && !CharPtr<T> &&
                !Optional<T> && !ArrayLike<T> && !CArrayLike<T> &&
                !AssociativeLike<T> && !std::is_same_v<Decay<T>, bool> &&
                !InitList<T>)
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
            else if constexpr (std::is_pointer_v<Decay<T>>)
            {
                static_assert(!std::is_pointer_v<Decay<T>>, "Unsupported pointer parameter (only char* allowed)");
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
        const char* values[N == 0 ? 1 : N];
        int lengths[N == 0 ? 1 : N];
        int formats[N == 0 ? 1 : N];
        Oid types[N == 0 ? 1 : N];

        std::vector<std::string> temp_strings;
        temp_strings.reserve(N > 0 ? N : 1);
        std::vector<std::vector<char>> temp_bytes;
        temp_bytes.reserve(N > 0 ? N : 1);

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
