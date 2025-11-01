#ifndef UREFLECT_AUTO_H
#define UREFLECT_AUTO_H

#include <array>
#include <string_view>
#include <type_traits>
#include <tuple>
#include <utility>

#if defined(__clang__) || defined(__GNUC__)
#define UREFL_PRETTY __PRETTY_FUNCTION__
#elif defined(_MSC_VER)
#define UREFL_PRETTY __FUNCSIG__
#else
#define UREFL_PRETTY __PRETTY_FUNCTION__
#endif

namespace ureflect {
    constexpr std::size_t sv_find_first(std::string_view s, char ch, std::size_t from = 0) {
        for (std::size_t i = from; i < s.size(); ++i) if (s[i] == ch) return i;
        return std::string_view::npos;
    }

    constexpr std::size_t sv_find_first_of(std::string_view s, std::string_view set, std::size_t from = 0) {
        for (std::size_t i = from; i < s.size(); ++i)
            for (char c: set) if (s[i] == c) return i;
        return std::string_view::npos;
    }

    constexpr std::size_t sv_find_token(std::string_view s, std::string_view tok, std::size_t from = 0) {
        if (tok.empty() || tok.size() > s.size()) return std::string_view::npos;
        for (std::size_t i = from; i + tok.size() <= s.size(); ++i) {
            bool ok = true;
            for (std::size_t j = 0; j < tok.size(); ++j)
                if (s[i + j] != tok[j]) {
                    ok = false;
                    break;
                }
            if (ok) return i;
        }
        return std::string_view::npos;
    }

    constexpr std::size_t sv_rfind_token(std::string_view s, std::string_view tok) {
        if (tok.empty() || tok.size() > s.size()) return std::string_view::npos;
        for (std::size_t p = s.size() - tok.size() + 1; p-- > 0;) {
            bool ok = true;
            for (std::size_t j = 0; j < tok.size(); ++j)
                if (s[p + j] != tok[j]) {
                    ok = false;
                    break;
                }
            if (ok) return p;
            if (p == 0) break;
        }
        return std::string_view::npos;
    }

    template<class T>
    consteval std::string_view type_name() {
        std::string_view f = UREFL_PRETTY;
#if defined(_MSC_VER) && !defined(__clang__)
        constexpr std::string_view key = "ureflect::type_name<";
        const auto k = sv_find_token(f, key);
        const auto b = (k == std::string_view::npos) ? 0 : k + key.size();
        const auto e = sv_find_first(f, '>', b);
        return (e == std::string_view::npos) ? std::string_view{} : f.substr(b, e - b);
#else
        const auto eq = sv_rfind_token(f, "T = ");
        const auto rb = sv_rfind_token(f, "]");
        const auto b = (eq == std::string_view::npos) ? 0 : eq + 4;
        const auto e = (rb == std::string_view::npos) ? f.size() : rb;
        return f.substr(b, e - b);
#endif
    }

    template<auto E>
        requires(std::is_enum_v<decltype(E)>)
    consteval std::string_view enum_name() {
        std::string_view f = UREFL_PRETTY;
#if defined(_MSC_VER) && !defined(__clang__)
        const auto pos = sv_rfind_token(f, "::");
        const auto end = sv_find_first(f, '>', pos == std::string_view::npos ? 0 : pos + 2);
        return (pos == std::string_view::npos || end == std::string_view::npos)
                   ? std::string_view{}
                   : f.substr(pos + 2, end - (pos + 2));
#else
        const auto eq = sv_rfind_token(f, "= ");
        const auto rb = sv_rfind_token(f, "]");
        const auto end = (rb == std::string_view::npos) ? f.size() : rb;
        const auto left = f.substr(0, end);
        const auto pos = sv_rfind_token(left, "::");
        const auto b = (pos == std::string_view::npos) ? (eq == std::string_view::npos ? 0 : eq + 2) : pos + 2;
        return f.substr(b, end - b);
#endif
    }

    struct any_t final {
        template<class X>
            requires(!std::same_as<std::remove_cvref_t<X>, const char *> &&
                     !std::same_as<std::remove_cvref_t<X>, std::nullptr_t>)
        constexpr operator X() const;

        constexpr operator std::string_view() const { return {}; }
    };

    template<class T, class... A>
        requires(std::is_aggregate_v<std::remove_cvref_t<T> >)
    inline constexpr auto count_members = [] {
        using V = std::remove_cvref_t<T>;
        if constexpr (requires { V{A{}..., any_t{}}; }) return count_members<V, A..., any_t>;
        else return sizeof...(A);
    }();

    template<class... Ts>
    struct tuple {
        std::tuple<Ts &...> refs;
    };

    template<class... Ts>
    constexpr auto tie(Ts &... xs) { return ::ureflect::tuple<Ts...>{std::tuple<Ts &...>(xs...)}; }

    template<std::size_t I, class... Ts>
    constexpr decltype(auto) get(::ureflect::tuple<Ts...> &t) { return std::get<I>(t.refs); }

    template<std::size_t I, class... Ts>
    constexpr decltype(auto) get(const ::ureflect::tuple<Ts...> &t) { return std::get<I>(t.refs); }

    template<std::size_t N, class T>
    struct tie_dispatch;

    template<class T>
    struct tie_dispatch<0, T> {
        static constexpr auto apply(T &) { return ::ureflect::tuple<>{}; }
    };

    template<class T>
    struct tie_dispatch<1, T> {
        static constexpr auto apply(T &v) {
            auto &[p0] = v;
            return ::ureflect::tie(p0);
        }
    };

    template<class T>
    struct tie_dispatch<2, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1] = v;
            return ::ureflect::tie(p0, p1);
        }
    };

    template<class T>
    struct tie_dispatch<3, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2] = v;
            return ::ureflect::tie(p0, p1, p2);
        }
    };

    template<class T>
    struct tie_dispatch<4, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2,p3] = v;
            return ::ureflect::tie(p0, p1, p2, p3);
        }
    };

    template<class T>
    struct tie_dispatch<5, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2,p3,p4] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4);
        }
    };

    template<class T>
    struct tie_dispatch<6, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2,p3,p4,p5] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5);
        }
    };

    template<class T>
    struct tie_dispatch<7, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2,p3,p4,p5,p6] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6);
        }
    };

    template<class T>
    struct tie_dispatch<8, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2,p3,p4,p5,p6,p7] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7);
        }
    };

    template<class T>
    struct tie_dispatch<9, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2,p3,p4,p5,p6,p7,p8] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8);
        }
    };

    template<class T>
    struct tie_dispatch<10, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
        }
    };

    template<class T>
    struct tie_dispatch<11, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10);
        }
    };

    template<class T>
    struct tie_dispatch<12, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11);
        }
    };

    template<class T>
    struct tie_dispatch<13, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12);
        }
    };

    template<class T>
    struct tie_dispatch<14, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13);
        }
    };

    template<class T>
    struct tie_dispatch<15, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14);
        }
    };

    template<class T>
    struct tie_dispatch<16, T> {
        static constexpr auto apply(T &v) {
            auto &[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15);
        }
    };

    template<class T, std::size_t N = count_members<T> >
        requires(N <= 16)
    constexpr auto to_tie(T &v) { return tie_dispatch<N, T>::apply(v); }

    template<class T, std::size_t N = count_members<T> >
        requires(N <= 16)
    constexpr auto to_tie(const T &v) { return ::ureflect::to_tie(const_cast<T &>(v)); }

    template<class T>
    extern const T uref_external;

    template<class T>
    struct ptr_box {
        const T *p;
    };

    template<std::size_t N, class T>
    consteval auto get_ptr(T &&t) {
        auto tiev = ::ureflect::to_tie(t);
        return ::ureflect::ptr_box<std::remove_reference_t<decltype(::ureflect::get<N>(tiev))> >{
            &::ureflect::get<N>(tiev)
        };
    }

    template<auto X>
    consteval std::string_view mangled_name() { return UREFL_PRETTY; }

    template<class T>
    consteval std::string_view mangled_name() { return UREFL_PRETTY; }

    struct UREFLECT_REFLECTOR {
        int UREFLECT_FIELD;
    };

    struct reflect_field {
        static constexpr std::string_view name =
                ::ureflect::mangled_name<&UREFLECT_REFLECTOR::UREFLECT_FIELD>();
        static constexpr std::size_t pos = ::ureflect::sv_find_token(name, "UREFLECT_FIELD");
        static constexpr char sep_before = (pos > 0) ? name[pos - 1] : ':';
        static constexpr char tail_after = (pos != std::string_view::npos &&
                                            pos + sizeof("UREFLECT_FIELD") - 1 < name.size())
                                               ? name[pos + sizeof("UREFLECT_FIELD") - 1]
                                               : ']';
    };

    struct reflect_type {
        static constexpr std::string_view name = ::ureflect::mangled_name<UREFLECT_REFLECTOR>();
#if defined(__GNUC__) || defined(__clang__)
        static constexpr std::string_view begin = "T = ";
#endif
    };

    template<std::size_t N, class T>
    struct member_nameof_impl {
        static constexpr std::string_view raw =
                ::ureflect::mangled_name<
                    ::ureflect::get_ptr<N>(::ureflect::uref_external<std::remove_volatile_t<T> >)
                >();

        static consteval std::string_view compute() {
            using namespace ::ureflect;
            constexpr std::string_view m1 = "&uref_external.";
            std::size_t k = sv_find_token(raw, m1);
            if (k != std::string_view::npos) {
                const std::size_t b = k + m1.size();
                const std::size_t e = sv_find_first_of(raw, std::string_view("}]>), ;"), b);
                return raw.substr(b, (e == std::string_view::npos ? raw.size() : e) - b);
            }

            constexpr std::string_view m2 = "uref_external.";
            k = sv_find_token(raw, m2);
            if (k != std::string_view::npos) {
                const std::size_t b = k + m2.size();
                const std::size_t e = sv_find_first_of(raw, std::string_view("}]>), ;"), b);
                return raw.substr(b, (e == std::string_view::npos ? raw.size() : e) - b);
            }

            const std::size_t end_pos = sv_find_token(raw, reflect_field::name.substr(
                                                          sv_find_token(reflect_field::name, "UREFLECT_FIELD") + sizeof(
                                                              "UREFLECT_FIELD") - 1));
            const std::string_view left = (end_pos == std::string_view::npos) ? raw : raw.substr(0, end_pos);

            std::size_t sep = std::string_view::npos;
            for (std::size_t i = 0; i < left.size(); ++i)
                if (left[i] == reflect_field::sep_before) sep = i;

            if (sep != std::string_view::npos) return left.substr(sep + 1);

            const std::size_t dbl = sv_rfind_token(left, "::");
            if (dbl != std::string_view::npos) return left.substr(dbl + 2);
            return {};
        }

        static constexpr std::string_view value = compute();
    };

    template<std::size_t N, class T>
    inline constexpr std::string_view member_nameof = member_nameof_impl<N, T>::value;


    template<class T, std::size_t... I>
    consteval auto member_names_impl(std::index_sequence<I...>) {
        if constexpr (sizeof...(I) == 0) return std::array<std::string_view, 0>{};
        else return std::array{::ureflect::member_nameof<I, T>...};
    }

    template<class T>
    inline constexpr auto member_names =
            member_names_impl<T>(std::make_index_sequence<count_members<T> >{});

    template<class T, class F>
    constexpr void for_each_field(T &obj, F &&fn) {
        constexpr std::size_t N = count_members<T>;
        if constexpr (N == 0) return;
        auto tiev = ::ureflect::to_tie(obj);
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            (fn(::ureflect::member_nameof<I, T>, ::ureflect::get<I>(tiev)), ...);
        }(std::make_index_sequence<N>{});
    }
} // namespace ureflect

#endif
