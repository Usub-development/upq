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
#include <cctype>

#include "uvent/Uvent.h"
#include "PgTypes.h"
#include "meta/PgConcepts.h"
#include "PgReflect.h" // is_tuple_like_v, ReflectAggregate, мапперы чтения

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

    static inline uint64_t extract_rows_affected(PGresult* res)
    {
        if (!res) return 0;
        if (const char* aff = PQcmdTuples(res); aff && *aff)
            return std::strtoull(aff, nullptr, 10);

        if (const char* tag = PQcmdStatus(res); tag && *tag)
        {
            const char* end = tag + std::strlen(tag);
            while (end > tag && std::isspace(static_cast<unsigned char>(*(end - 1)))) --end;
            const char* p = end;
            while (p > tag && std::isdigit(static_cast<unsigned char>(*(p - 1)))) --p;
            if (p < end) return std::strtoull(p, nullptr, 10);
        }
        return 0;
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
            lengths[i] = static_cast<int>(sv.size());
            formats[i] = 0;
            types[i] = 0; // TEXT inferred
        }

        void set_text_typed(std::string_view sv, Oid oid)
        {
            const auto i = (*idx)++;
            temp_strings.emplace_back(sv);
            values[i] = temp_strings.back().c_str();
            lengths[i] = static_cast<int>(sv.size());
            formats[i] = 0; // text
            types[i] = oid; // explicit
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
            lengths[i] = static_cast<int>(n);
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

        // ----- контейнеры → PG array (один параметр) -----
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

        template <class T, size_t N>
        inline void encode_one(ParamSlices& ps, T (&arr)[N])
        {
            encode_one(ps, const_cast<const T(&)[N]>(arr));
        }

        template <class T>
        inline void encode_one(ParamSlices& ps, std::initializer_list<T> il)
        {
            using Elem = unopt_t<T>;
            constexpr Oid arr_oid = pick_array_oid<Elem>();
            const std::string s = build_pg_array_from_range(il);
            ps.set_text_typed(s, arr_oid);
        }

        // ---- tuple-like → разворачиваем элементы в несколько SQL-параметров
        template <class Tup>
            requires (::usub::pg::detail::is_tuple_like_v<std::decay_t<Tup>> &&
                !ArrayLike<std::decay_t<Tup>> &&
                !CArrayLike<std::decay_t<Tup>>)
        inline void encode_one(ParamSlices& ps, const Tup& tup)
        {
            using DT = std::decay_t<Tup>;
            [&]<std::size_t... I>(std::index_sequence<I...>)
            {
                (encode_one(ps, std::get<I>(tup)), ...);
            }(std::make_index_sequence<std::tuple_size_v<DT>>{});
        }

        // ---- рефлексируемый агрегат → разворачиваем поля в несколько SQL-параметров
        template <class T>
            requires (::usub::pg::detail::ReflectAggregate<std::decay_t<T>> &&
                !::usub::pg::detail::is_tuple_like_v<std::decay_t<T>> &&
                !ArrayLike<std::decay_t<T>> &&
                !CArrayLike<std::decay_t<T>>)
        inline void encode_one(ParamSlices& ps, const T& obj)
        {
            using V = std::decay_t<T>;
            auto& nonconst = const_cast<V&>(obj);
            auto tiev = ureflect::to_tie(nonconst);
            [&]<std::size_t... I>(std::index_sequence<I...>)
            {
                (encode_one(ps, ureflect::get<I>(tiev)), ...);
            }(std::make_index_sequence<ureflect::count_members<V>>{});
        }

        // --- fallback ---
        template <class T>
            requires (!Integral<T> && !Floating<T> && !StringLike<T> && !CharPtr<T> &&
                !Optional<T> && !ArrayLike<T> && !CArrayLike<T> &&
                !AssociativeLike<T> && !std::is_same_v<std::decay_t<T>, bool> &&
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
            else if constexpr (std::is_pointer_v<std::decay_t<T>>)
            {
                static_assert(!std::is_pointer_v<std::decay_t<T>>,
                              "Unsupported pointer parameter (only char* allowed)");
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
                oss << "<param:" << typeid(std::decay_t<T>).name() << ">";
                ps.set_text(oss.str());
            }
        }

        // ---- param arity ----
        template <class T>
        struct param_arity_impl
        {
            static constexpr size_t value = 1;
        };

        template <class Opt>
            requires Optional<Opt>
        struct param_arity_impl<Opt>
        {
            static constexpr size_t value = 1;
        };

        template <class C>
            requires ArrayLike<C>
        struct param_arity_impl<C>
        {
            static constexpr size_t value = 1;
        };

        template <class T, size_t N>
        struct param_arity_impl<T[N]>
        {
            static constexpr size_t value = 1;
        };

        template <class T>
        struct param_arity_impl<std::initializer_list<T>>
        {
            static constexpr size_t value = 1;
        };

        template <class Tup>
            requires (::usub::pg::detail::is_tuple_like_v<Tup> &&
                !ArrayLike<Tup> &&
                !CArrayLike<Tup>)
        struct param_arity_impl<Tup>
        {
            static constexpr size_t value = std::tuple_size_v<std::decay_t<Tup>>;
        };

        template <class T>
            requires (::usub::pg::detail::ReflectAggregate<T> &&
                !::usub::pg::detail::is_tuple_like_v<T> &&
                !ArrayLike<T> &&
                !CArrayLike<T>)
        struct param_arity_impl<T>
        {
            static constexpr size_t value = ureflect::count_members<std::decay_t<T>>;
        };

        template <class T>
        struct param_arity : param_arity_impl<std::decay_t<T>>
        {
        };

        template <class... Args>
        constexpr size_t count_total_params()
        {
            return (param_arity<Args>::value + ... + 0);
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

        template <typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        exec_param_query_nonblocking(const std::string& sql, Args&&... args);

        // reflect-aware SELECT → std::vector<T>
        template <class T>
        usub::uvent::task::Awaitable<std::vector<T>>
        exec_simple_query_nonblocking(const std::string& sql)
        {
            using Fn = usub::uvent::task::Awaitable<usub::pg::QueryResult>
                (PgConnectionLibpq::*)(const std::string&);
            Fn base = &PgConnectionLibpq::exec_simple_query_nonblocking;

            usub::pg::QueryResult qr = co_await (this->*base)(sql);
            co_return usub::pg::map_all_reflect_positional<T>(qr);
        }

        // reflect-aware SELECT one → std::optional<T>
        template <class T>
        usub::uvent::task::Awaitable<std::optional<T>>
        exec_simple_query_one_nonblocking(const std::string& sql)
        {
            using Fn = usub::uvent::task::Awaitable<usub::pg::QueryResult>
                (PgConnectionLibpq::*)(const std::string&);
            Fn base = &PgConnectionLibpq::exec_simple_query_nonblocking;

            usub::pg::QueryResult qr = co_await (this->*base)(sql);
            if (qr.rows.empty()) co_return std::nullopt;
            co_return usub::pg::map_single_reflect_positional<T>(qr, 0);
        }

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

    template <typename... Args>
    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::exec_param_query_nonblocking(const std::string& sql, Args&&... args)
    {
        QueryResult out{};
        out.ok = false;
        out.rows_affected = 0;

        if (!connected())
        {
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            co_return out;
        }

        // учитываем tuple/aggregate, массивы и т.п.
        constexpr size_t M = detail::count_total_params<Args...>();

        std::vector<const char*> values(M ? M : 1, nullptr);
        std::vector<int> lengths(M ? M : 1, 0);
        std::vector<int> formats(M ? M : 1, 0);
        std::vector<Oid> types(M ? M : 1, 0);

        std::vector<std::string> temp_strings;
        temp_strings.reserve(M ? M : 1);
        std::vector<std::vector<char>> temp_bytes;
        temp_bytes.reserve(M ? M : 1);

        size_t idx = 0;
        ParamSlices ps{
            values.data(), lengths.data(), formats.data(), types.data(), &idx,
            temp_strings, temp_bytes
        };

        // каждый аргумент может добавить 1..N params
        (detail::encode_one(ps, std::forward<Args>(args)), ...);

        const int nParams = static_cast<int>(idx);

        if (!PQsendQueryParams(conn_, sql.c_str(), nParams,
                               types.data(), values.data(), lengths.data(), formats.data(), 0))
        {
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            co_return out;
        }

        // flush
        for (;;)
        {
            const int fr = PQflush(conn_);
            if (fr == 0) break;
            if (fr == -1)
            {
                out.code = PgErrorCode::SocketReadFailed;
                out.error = PQerrorMessage(conn_);
                co_return out;
            }
            co_await wait_writable();
        }

        // pump & drain
        for (;;)
        {
            if (PQconsumeInput(conn_) == 0)
            {
                out.code = PgErrorCode::SocketReadFailed;
                out.error = PQerrorMessage(conn_);
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
                    if (out.rows_affected == 0)
                        out.rows_affected = static_cast<uint64_t>(nrows);
                }
                else if (st == PGRES_COMMAND_OK)
                {
                    out.ok = true;
                    out.code = PgErrorCode::OK;
                    out.rows_affected += extract_rows_affected(res);
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
                }
                co_return out;
            }
            co_await wait_readable();
        }
    }
} // namespace usub::pg

#endif // PGCONNECTIONLIBPQ_H
