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
#include <cctype>
#include <algorithm>

#include "ureflect/ureflect_auto.h"
#include "PgTypes.h"

#ifndef UPQ_REFLECT_DEBUG
#define UPQ_REFLECT_DEBUG 0
#endif

#if UPQ_REFLECT_DEBUG
  #ifndef UPQ_LOG
  #define UPQ_LOG(fmt, ...) std::printf(fmt "\n", ##__VA_ARGS__)
  #endif
#else
  #ifndef UPQ_LOG
  #define UPQ_LOG(...) ((void)0)
  #endif
#endif

namespace usub::pg
{
    namespace detail
    {
        // ---- small logging helpers ----
        inline std::string join_csv(const std::vector<std::string>& v) {
            std::string out; out.reserve(64);
            for (size_t i = 0; i < v.size(); ++i) {
                if (i) out += ", ";
                out += v[i];
            }
            return out;
        }

        // ---- ident normalization: keep [a-z0-9_], to lower ----
        inline std::string normalize_ident(std::string_view in)
        {
            std::string s; s.reserve(in.size());
            for (char ch : in) {
                unsigned char c = static_cast<unsigned char>(ch);
                if (std::isalnum(c)) {
                    s.push_back(static_cast<char>(std::tolower(c)));
                } else if (ch == '_') {
                    s.push_back('_');
                } else {
                    // drop
                }
            }
            // collapse multiple '_' (optional)
            std::string out; out.reserve(s.size());
            bool prev_us = false;
            for (char ch : s) {
                if (ch == '_') {
                    if (!prev_us) out.push_back('_');
                    prev_us = true;
                } else {
                    out.push_back(ch);
                    prev_us = false;
                }
            }
            return out;
        }

        // tuple-like detector
        template <class T, class = void>
        struct is_tuple_like : std::false_type {};
        template <class T>
        struct is_tuple_like<T, std::void_t<decltype(std::tuple_size<std::decay_t<T>>::value)>> : std::true_type {};
        template <class T>
        inline constexpr bool is_tuple_like_v = is_tuple_like<T>::value;

        // aggregate reflectable via ureflect
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
            if (res.ec == std::errc() && res.ptr == end) { out = tmp; return true; }
            return false;
        }

        inline bool parse_bool(std::string_view sv, bool& out) noexcept
        {
            if (sv == "t" || sv == "true" || sv == "1") { out = true; return true; }
            if (sv == "f" || sv == "false" || sv == "0") { out = false; return true; }
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
            if (res.ec == std::errc() && res.ptr == end) { out = tmp; return true; }
            return false;
        }

        // PG array split: {a,b,"c,d","e""e",NULL}
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
                    if (c == '"' && i + 1 < s.size() && s[i + 1] == '"') { ++i; continue; }
                    if (c == '"') { inq = false; continue; }
                }
                else
                {
                    if (c == '"') { inq = true; continue; }
                    if (c == ',') { items.emplace_back(s.substr(start, i - start)); start = i + 1; }
                }
            }
            if (start <= s.size() - 1) items.emplace_back(s.substr(start, s.size() - 1 - start));
            return items;
        }

        inline bool parse_pg_text_elt(std::string_view sv, std::string& out, bool& is_null)
        {
            is_null = false;
            if (sv == "NULL") { is_null = true; out.clear(); return true; }
            if (!sv.empty() && sv.front() == '"' && sv.back() == '"')
            {
                out.clear();
                out.reserve(sv.size());
                for (size_t i = 1; i + 1 < sv.size(); ++i)
                {
                    char c = sv[i];
                    if (c == '"' && i + 1 < sv.size() && sv[i + 1] == '"') { out.push_back('"'); ++i; }
                    else out.push_back(c);
                }
                return true;
            }
            out.assign(sv.begin(), sv.end());
            return true;
        }

        // --------------------- Decoder<T> ---------------------
        template <class T, class Enable = void>
        struct Decoder
        {
            static bool apply(std::string_view sv, T& out)
            {
                if constexpr (std::is_same_v<T, std::string>)
                {
                    out.assign(sv.data(), sv.size()); return true;
                }
                else if constexpr (std::is_same_v<T, std::string_view>)
                {
                    out = sv; return true;
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
                    if ((iss >> tmp) && fully_consumed(iss)) { out = std::move(tmp); return true; }
                    return false;
                }
            }
        };

        template <class T>
        struct Decoder<std::optional<T>, void>
        {
            static bool apply(std::string_view sv, std::optional<T>& out)
            {
                if (sv.size() == 0) { out.reset(); return true; } // driver yields "" for NULL
                T v{};
                if (Decoder<T>::apply(sv, v)) { out = std::move(v); return true; }
                return false;
            }
        };

        template <class T> struct is_std_vector : std::false_type {};
        template <class U, class A> struct is_std_vector<std::vector<U, A>> : std::true_type {};

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
                    if (is_null) { out.emplace_back(T{}); continue; }

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

        // --------------------- positional fillers (fallback) ---------------------
        template <class Tuple>
            requires is_tuple_like_v<Tuple>
        inline bool fill_from_row_positional(const QueryResult::Row& row, Tuple& dst, std::string* err)
        {
            using Tup = std::decay_t<Tuple>;
            constexpr std::size_t N = std::tuple_size_v<Tup>;
            if (row.cols.size() < N) { if (err) *err = "not enough columns"; return false; }

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
            if (row.cols.size() < N) { if (err) *err = "not enough columns"; return false; }

            auto tie = ureflect::to_tie(dst);
            bool ok = true;
            [&]<std::size_t... I>(std::index_sequence<I...>)
            {
                ( [&]
                {
                    using FieldT = std::remove_reference_t<decltype(ureflect::get<I>(tie))>;
                    const auto& sv = row.cols[I];
                    FieldT tmp{};
                    if (!Decoder<FieldT>::apply(std::string_view(sv.data(), sv.size()), tmp))
                        ok = false;
                    else
                        ureflect::get<I>(tie) = std::move(tmp);
                }(), ... );
            }(std::make_index_sequence<N>{});
            if (!ok && err && err->empty()) *err = "failed to decode aggregate field";
            return ok;
        }

        inline int find_col_idx(const std::vector<std::string>& cols, std::string_view norm_name)
        {
            // cols — уже нормализованные имена
            for (size_t i = 0; i < cols.size(); ++i)
                if (cols[i] == norm_name) return static_cast<int>(i);
            return -1;
        }

        // --------------------- name-based for aggregates ---------------------
        template <class T>
            requires ReflectAggregate<T>
        inline bool fill_from_row_named(const QueryResult& qr, size_t row_index, T& dst, std::string* err)
        {
            using V = std::decay_t<T>;
            constexpr std::size_t N = ureflect::count_members<V>;

            if (row_index >= qr.rows.size()) { if (err) *err = "row out of range"; return false; }
            const auto& row = qr.rows[row_index];

            if (qr.columns.empty())
            {
                if (err) *err = "columns are empty (driver didn't fill names)";
                return false;
            }

            // 1) normalize all column names once
            std::vector<std::string> norm_cols;
            norm_cols.reserve(qr.columns.size());
            for (auto& c : qr.columns) norm_cols.emplace_back(normalize_ident(c));

#if UPQ_REFLECT_DEBUG
            UPQ_LOG("[UPQ/reflect] columns[%zu]: %s", norm_cols.size(), join_csv(qr.columns).c_str());
#endif

            // 2) build normalized field names from ureflect::member_names<V>
            constexpr auto fnames = ureflect::member_names<V>;
            std::array<std::string, N> norm_fields{};
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                ( (norm_fields[I] = normalize_ident(fnames[I])), ... );
            }(std::make_index_sequence<N>{});

#if UPQ_REFLECT_DEBUG
            {
                std::vector<std::string> raw_fields; raw_fields.reserve(N);
                std::vector<std::string> norm_fields_vec; norm_fields_vec.reserve(N);
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    ( (raw_fields.emplace_back(std::string(fnames[I])),
                        norm_fields_vec.emplace_back(norm_fields[I])), ... );
                }(std::make_index_sequence<N>{});
                UPQ_LOG("[UPQ/reflect] fields[%zu]: %s", raw_fields.size(), join_csv(raw_fields).c_str());
                UPQ_LOG("[UPQ/reflect] fields_norm[%zu]: %s", norm_fields_vec.size(), join_csv(norm_fields_vec).c_str());
            }
#endif

            int col_map[N];
            bool all_found = true;

            [&]<std::size_t... I>(std::index_sequence<I...>)
            {
                (([&]{
                    const int idx = find_col_idx(norm_cols, norm_fields[I]);
#if UPQ_REFLECT_DEBUG
                    UPQ_LOG("[UPQ/reflect] map field '%s' -> col %d",
                            norm_fields[I].c_str(), idx);
#endif
                    col_map[I] = idx;
                    if (idx < 0) all_found = false;
                }()), ...);
            }(std::make_index_sequence<N>{});

            if (!all_found)
            {
                if (err) *err = "not all fields matched by name";
                return false;
            }

            auto tie = ureflect::to_tie(dst);
            bool ok = true;
            [&]<std::size_t... I>(std::index_sequence<I...>)
            {
                ( [&]
                {
                    using FieldT = std::remove_reference_t<decltype(ureflect::get<I>(tie))>;
                    const int c = col_map[I];
                    const auto& sv = row.cols[static_cast<size_t>(c)];
                    FieldT tmp{};
                    if (!Decoder<FieldT>::apply(std::string_view(sv.data(), sv.size()), tmp))
                        ok = false;
                    else
                        ureflect::get<I>(tie) = std::move(tmp);
                }(), ... );
            }(std::make_index_sequence<N>{});

            if (!ok && err && !err->size()) *err = "failed to decode aggregate field (named)";
            return ok;
        }
    } // namespace detail

    // --------------------- public API: positional (оставляем) ---------------------
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

    // --------------------- public API: named (АГРЕГАТЫ) ---------------------
    template <class T>
        requires detail::ReflectAggregate<T>
    inline bool map_row_reflect_named(const QueryResult& qr, size_t row_index, T& out, std::string* err = nullptr)
    {
        return detail::fill_from_row_named(qr, row_index, out, err);
    }

    template <class T>
        requires detail::ReflectAggregate<T>
    inline T map_single_reflect_named(const QueryResult& qr, size_t row = 0, std::string* err = nullptr)
    {
        if (row >= qr.rows.size()) throw std::out_of_range("row index out of range");
        T dst{};
        if (!map_row_reflect_named(qr, row, dst, err))
            throw std::runtime_error(err && !err->empty() ? *err : "decode failed");
        return dst;
    }

    template <class T>
        requires detail::ReflectAggregate<T>
    inline std::vector<T> map_all_reflect_named(const QueryResult& qr, std::string* err = nullptr)
    {
        std::vector<T> out;
        out.reserve(qr.rows.size());
        for (size_t i = 0; i < qr.rows.size(); ++i)
        {
            T dst{};
            if (!map_row_reflect_named(qr, i, dst, err))
                throw std::runtime_error(err && !err->empty() ? *err : "decode failed");
            out.emplace_back(std::move(dst));
        }
        return out;
    }
} // namespace usub::pg

#endif // PGREFLECT_H
