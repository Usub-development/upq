#ifndef PGREFLECT_H
#define PGREFLECT_H

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include <sstream>
#include <cstdio>
#include <ios>
#include <istream>

#include "meta/ureflect_auto.h"
#include "PgTypes.h"

namespace usub::pg
{
    struct NamedResult
    {
        std::vector<std::string> columns;
        QueryResult result;
    };

    namespace detail
    {
        // --- traits ---
        template <class T, class = void>
        struct is_tuple_like : std::false_type
        {
        };

        template <class T>
        struct is_tuple_like<T, std::void_t<decltype(std::tuple_size<std::decay_t<T>>::value)>> : std::true_type
        {
        };

        template <class T>
        inline constexpr bool is_tuple_like_v = is_tuple_like<T>::value;

        template <class T>
        concept ReflectAggregate =
            std::is_aggregate_v<std::decay_t<T>> &&
            (ureflect::count_members<std::decay_t<T>> > 0);

        inline bool fully_consumed(std::istringstream& iss)
        {
            iss >> std::ws;
            return iss.peek() == std::char_traits<char>::eof();
        }

        // --- primitive parsers ---
        template <class Int>
        inline bool parse_int(std::string_view sv, Int& out) noexcept
        {
            Int tmp{};
            const char* begin = sv.data();
            const char* end = sv.data() + sv.size();
            auto res = std::from_chars(begin, end, tmp);
            if (res.ec == std::errc() && res.ptr == end)
            {
                out = tmp;
                return true;
            }
            return false;
        }

        inline bool parse_bool(std::string_view sv, bool& out) noexcept
        {
            if (sv == "t" || sv == "true" || sv == "1")
            {
                out = true;
                return true;
            }
            if (sv == "f" || sv == "false" || sv == "0")
            {
                out = false;
                return true;
            }
            return false;
        }

        template <class Float>
        inline bool parse_float(std::string_view sv, Float& out) noexcept
        {
            if (sv.empty()) return false;
            Float tmp{};
            const char* begin = sv.data();
            const char* end = sv.data() + sv.size();
            auto res = std::from_chars(begin, end, tmp);
            if (res.ec == std::errc() && res.ptr == end)
            {
                out = tmp;
                return true;
            }
            return false;
        }

        // --- PG array literal parsing (text mode) ---
        inline std::vector<std::string_view> split_pg_array_items(std::string_view s)
        {
            std::vector<std::string_view> items;
            if (s.size() < 2 || s.front() != '{' || s.back() != '}') return items;
            size_t i = 1, start = 1;
            bool inq = false;
            for (; i + 1 < s.size(); ++i)
            {
                char c = s[i];
                if (inq)
                {
                    if (c == '"' && i + 1 < s.size() && s[i + 1] == '"')
                    {
                        ++i;
                        continue;
                    }
                    if (c == '"')
                    {
                        inq = false;
                        continue;
                    }
                }
                else
                {
                    if (c == '"')
                    {
                        inq = true;
                        continue;
                    }
                    if (c == ',')
                    {
                        items.emplace_back(s.substr(start, i - start));
                        start = i + 1;
                    }
                }
            }
            if (start <= s.size() - 1) items.emplace_back(s.substr(start, s.size() - 1 - start));
            return items;
        }

        inline bool parse_pg_text_elt(std::string_view sv, std::string& out, bool& is_null)
        {
            is_null = false;
            if (sv == "NULL")
            {
                is_null = true;
                out.clear();
                return true;
            }
            if (!sv.empty() && sv.front() == '"' && sv.back() == '"')
            {
                out.clear();
                out.reserve(sv.size());
                for (size_t i = 1; i + 1 < sv.size(); ++i)
                {
                    char c = sv[i];
                    if (c == '"' && i + 1 < sv.size() && sv[i + 1] == '"')
                    {
                        out.push_back('"');
                        ++i;
                    }
                    else out.push_back(c);
                }
                return true;
            }
            out.assign(sv.begin(), sv.end());
            return true;
        }

        // --- Decoder<T> ---
        template <class T, class Enable = void>
        struct Decoder
        {
            static bool apply(std::string_view sv, T& out)
            {
                if constexpr (std::is_same_v<T, std::string>)
                {
                    out.assign(sv.data(), sv.size());
                    return true;
                }
                else if constexpr (std::is_same_v<T, std::string_view>)
                {
                    out = sv;
                    return true;
                }
                else if constexpr (std::is_same_v<T, bool>)
                {
                    return parse_bool(sv, out);
                }
                else if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>)
                {
                    return parse_int<T>(sv, out);
                }
                else if constexpr (std::is_floating_point_v<T>)
                {
                    return parse_float<T>(sv, out);
                }
                else
                {
                    std::istringstream iss{std::string(sv)};
                    T tmp{};
                    if ((iss >> tmp) && fully_consumed(iss))
                    {
                        out = std::move(tmp);
                        return true;
                    }
                    return false;
                }
            }
        };

        template <class T>
        struct Decoder<std::optional<T>, void>
        {
            static bool apply(std::string_view sv, std::optional<T>& out)
            {
                if (sv.data() == nullptr || sv.size() == 0)
                {
                    out.reset();
                    return true;
                }
                T v{};
                if (Decoder<T>::apply(sv, v))
                {
                    out = std::move(v);
                    return true;
                }
                return false;
            }
        };

        template <class T>
        struct is_std_vector : std::false_type
        {
        };

        template <class U, class A>
        struct is_std_vector<std::vector<U, A>> : std::true_type
        {
        };

        template <class VecT>
        struct Decoder<VecT, std::enable_if_t<is_std_vector<VecT>::value>>
        {
            using T = typename VecT::value_type;

            static bool apply(std::string_view sv, VecT& out)
            {
                out.clear();
                auto parts = split_pg_array_items(sv);
                if (parts.empty() && !(sv.size() >= 2 && sv.front() == '{' && sv.back() == '}'))
                    return false;
                out.reserve(parts.size());
                for (auto p : parts)
                {
                    std::string tok;
                    bool is_null = false;
                    if (!parse_pg_text_elt(p, tok, is_null)) return false;
                    if (is_null)
                    {
                        out.emplace_back(T{});
                        continue;
                    }

                    if constexpr (std::is_same_v<T, std::string>)
                        out.emplace_back(std::move(tok));
                    else if constexpr (std::is_integral_v<T>)
                    {
                        T v{};
                        if (!parse_int<T>(tok, v)) return false;
                        out.emplace_back(v);
                    }
                    else if constexpr (std::is_floating_point_v<T>)
                    {
                        T v{};
                        if (!parse_float<T>(tok, v)) return false;
                        out.emplace_back(v);
                    }
                    else
                    {
                        std::istringstream iss(tok);
                        T v{};
                        if (!(iss >> v) || !fully_consumed(iss)) return false;
                        out.emplace_back(std::move(v));
                    }
                }
                return true;
            }
        };

        // --- row -> T (positional) ---
        template <class Tuple>
            requires is_tuple_like_v<Tuple>
        inline bool fill_from_row_positional(const QueryResult::Row& row, Tuple& dst, std::string* err)
        {
            using Tup = std::decay_t<Tuple>;
            constexpr std::size_t N = std::tuple_size_v<Tup>;
            if (row.cols.size() < N)
            {
                if (err) *err = "not enough columns";
                return false;
            }

            bool ok = true;
            [&]<std::size_t... I>(std::index_sequence<I...>)
            {
                ( [&]
                {
                    using Elem = std::tuple_element_t<I, Tup>;
                    const auto& sv = row.cols[I];
                    Elem tmp{};
                    if (!Decoder<Elem>::apply(std::string_view(sv.data(), sv.size()), tmp))
                        ok = false;
                    else
                        std::get<I>(dst) = std::move(tmp);
                }(), ... );
            }(std::make_index_sequence<N>{});
            if (!ok && err && err->empty()) *err = "failed to decode tuple element";
            return ok;
        }

        template <class T>
            requires ReflectAggregate<T>
        inline bool fill_from_row_positional(const QueryResult::Row& row, T& dst, std::string* err)
        {
            using V = std::decay_t<T>;
            constexpr std::size_t N = ureflect::count_members<V>;
            if (row.cols.size() < N)
            {
                if (err) *err = "not enough columns";
                return false;
            }

            auto tiev = ureflect::to_tie(dst);
            bool ok = true;
            [&]<std::size_t... I>(std::index_sequence<I...>)
            {
                ( [&]
                {
                    using FieldT = std::remove_reference_t<decltype(ureflect::get<I>(tiev))>;
                    const auto& sv = row.cols[I];
                    FieldT tmp{};
                    if (!Decoder<FieldT>::apply(std::string_view(sv.data(), sv.size()), tmp))
                        ok = false;
                    else
                        ureflect::get<I>(tiev) = std::move(tmp);
                }(), ... );
            }(std::make_index_sequence<N>{});
            if (!ok && err && err->empty()) *err = "failed to decode aggregate field";
            return ok;
        }

        inline int find_col_idx(const std::vector<std::string>& cols, std::string_view name)
        {
            for (size_t i = 0; i < cols.size(); ++i)
                if (cols[i] == name) return static_cast<int>(i);
            return -1;
        }
    } // namespace detail

    // --- Public mapping API (positional) ---
    template <class T>
    inline bool map_row_reflect_positional(const QueryResult::Row& row, T& out, std::string* err = nullptr)
    {
        if constexpr (detail::is_tuple_like_v<T>)
            return detail::fill_from_row_positional(row, out, err);
        else
            return detail::fill_from_row_positional(row, out, err);
    }

    template <class T>
    inline T map_single_reflect_positional(const QueryResult& qr, size_t row = 0, std::string* err = nullptr)
    {
        if (row >= qr.rows.size()) throw std::out_of_range("row index out of range");
        T dst{};
        if (!map_row_reflect_positional(qr.rows[row], dst, err))
            throw std::runtime_error(err && !err->empty() ? *err : "decode failed");
        return dst;
    }

    template <class T>
    inline std::vector<T> map_all_reflect_positional(const QueryResult& qr, std::string* err = nullptr)
    {
        std::vector<T> out;
        out.reserve(qr.rows.size());
        for (const auto& r : qr.rows)
        {
            T dst{};
            if (!map_row_reflect_positional(r, dst, err))
                throw std::runtime_error(err && !err->empty() ? *err : "decode failed");
            out.emplace_back(std::move(dst));
        }
        return out;
    }

    // --- Reflect DML passthrough (insert/update with aggregate/tuple) ---
    template <class T>
        requires detail::ReflectAggregate<T>
    inline usub::uvent::task::Awaitable<QueryResult>
    exec_param_query_nonblocking_reflect(PgConnectionLibpq& conn, const std::string& sql, const T& obj)
    {
        co_return co_await conn.exec_param_query_nonblocking(sql, obj);
    }

    template <class Tuple>
        requires (detail::is_tuple_like_v<Tuple>)
    inline usub::uvent::task::Awaitable<QueryResult>
    exec_param_query_nonblocking_reflect(PgConnectionLibpq& conn, const std::string& sql, const Tuple& tup)
    {
        co_return co_await conn.exec_param_query_nonblocking(sql, tup);
    }

    template <class T, class F>
        requires detail::ReflectAggregate<T>
    inline usub::uvent::task::Awaitable<QueryResult>
    exec_param_query_nonblocking_reflect_build(PgConnectionLibpq& conn, const std::string& sql, F&& fill)
    {
        T obj{};
        fill(obj);
        co_return co_await conn.exec_param_query_nonblocking(sql, obj);
    }
} // namespace usub::pg

#endif // PGREFLECT_H
