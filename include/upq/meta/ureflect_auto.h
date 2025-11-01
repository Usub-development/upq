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

namespace ureflect
{
    constexpr std::size_t sv_find_first(std::string_view s, char ch, std::size_t from = 0)
    {
        for (std::size_t i = from; i < s.size(); ++i) if (s[i] == ch) return i;
        return std::string_view::npos;
    }

    constexpr std::size_t sv_find_first_of(std::string_view s, std::string_view set, std::size_t from = 0)
    {
        for (std::size_t i = from; i < s.size(); ++i)
            for (char c : set) if (s[i] == c) return i;
        return std::string_view::npos;
    }

    constexpr std::size_t sv_find_token(std::string_view s, std::string_view tok, std::size_t from = 0)
    {
        if (tok.empty() || tok.size() > s.size()) return std::string_view::npos;
        for (std::size_t i = from; i + tok.size() <= s.size(); ++i)
        {
            bool ok = true;
            for (std::size_t j = 0; j < tok.size(); ++j)
                if (s[i + j] != tok[j])
                {
                    ok = false;
                    break;
                }
            if (ok) return i;
        }
        return std::string_view::npos;
    }

    constexpr std::size_t sv_rfind_token(std::string_view s, std::string_view tok)
    {
        if (tok.empty() || tok.size() > s.size()) return std::string_view::npos;
        for (std::size_t p = s.size() - tok.size() + 1; p-- > 0;)
        {
            bool ok = true;
            for (std::size_t j = 0; j < tok.size(); ++j)
                if (s[p + j] != tok[j])
                {
                    ok = false;
                    break;
                }
            if (ok) return p;
            if (p == 0) break;
        }
        return std::string_view::npos;
    }

    template <class T>
    consteval std::string_view type_name()
    {
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

    template <auto E>
        requires(std::is_enum_v<decltype(E)>)
    consteval std::string_view enum_name()
    {
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

    struct any_t final
    {
        template <class X>
            requires(!std::same_as<std::remove_cvref_t<X>, const char*> &&
                !std::same_as<std::remove_cvref_t<X>, std::nullptr_t>)
        constexpr operator X() const;

        constexpr operator std::string_view() const { return {}; }
    };

    template <class T, class... A>
        requires(std::is_aggregate_v<std::remove_cvref_t<T>>)
    inline constexpr auto count_members = []
    {
        using V = std::remove_cvref_t<T>;
        if constexpr (requires { V{A{}..., any_t{}}; }) return count_members<V, A..., any_t>;
        else return sizeof...(A);
    }();

    template <class... Ts>
    struct tuple
    {
        std::tuple<Ts&...> refs;
    };

    template <class... Ts>
    constexpr auto tie(Ts&... xs) { return ::ureflect::tuple<Ts...>{std::tuple<Ts&...>(xs...)}; }

    template <std::size_t I, class... Ts>
    constexpr decltype(auto) get(::ureflect::tuple<Ts...>& t) { return std::get<I>(t.refs); }

    template <std::size_t I, class... Ts>
    constexpr decltype(auto) get(const ::ureflect::tuple<Ts...>& t) { return std::get<I>(t.refs); }

    template <std::size_t N, class T>
    struct tie_dispatch;

    template <class T>
    struct tie_dispatch<0, T>
    {
        static constexpr auto apply(T&) { return ::ureflect::tuple<>{}; }
    };

    template <class T>
    struct tie_dispatch<1, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0] = v;
            return ::ureflect::tie(p0);
        }
    };

    template <class T>
    struct tie_dispatch<2, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1] = v;
            return ::ureflect::tie(p0, p1);
        }
    };

    template <class T>
    struct tie_dispatch<3, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2] = v;
            return ::ureflect::tie(p0, p1, p2);
        }
    };

    template <class T>
    struct tie_dispatch<4, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3] = v;
            return ::ureflect::tie(p0, p1, p2, p3);
        }
    };

    template <class T>
    struct tie_dispatch<5, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4);
        }
    };

    template <class T>
    struct tie_dispatch<6, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5);
        }
    };

    template <class T>
    struct tie_dispatch<7, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6);
        }
    };

    template <class T>
    struct tie_dispatch<8, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7);
        }
    };

    template <class T>
    struct tie_dispatch<9, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8);
        }
    };

    template <class T>
    struct tie_dispatch<10, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
        }
    };

    template <class T>
    struct tie_dispatch<11, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10);
        }
    };

    template <class T>
    struct tie_dispatch<12, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11);
        }
    };

    template <class T>
    struct tie_dispatch<13, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12);
        }
    };

    template <class T>
    struct tie_dispatch<14, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13);
        }
    };

    template <class T>
    struct tie_dispatch<15, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14);
        }
    };

    template <class T>
    struct tie_dispatch<16, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15);
        }
    };

    template <class T>
    struct tie_dispatch<17, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16);
        }
    };

    template <class T>
    struct tie_dispatch<18, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17);
        }
    };

    template <class T>
    struct tie_dispatch<19, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18);
        }
    };

    template <class T>
    struct tie_dispatch<20, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19);
        }
    };

    template <class T>
    struct tie_dispatch<21, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20);
        }
    };

    template <class T>
    struct tie_dispatch<22, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21);
        }
    };

    template <class T>
    struct tie_dispatch<23, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22);
        }
    };

    template <class T>
    struct tie_dispatch<24, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23);
        }
    };

    template <class T>
    struct tie_dispatch<25, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24);
        }
    };

    template <class T>
    struct tie_dispatch<26, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25);
        }
    };

    template <class T>
    struct tie_dispatch<27, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26] =
                v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26);
        }
    };

    template <class T>
    struct tie_dispatch<28, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,
                p27] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27);
        }
    };

    template <class T>
    struct tie_dispatch<29, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28);
        }
    };

    template <class T>
    struct tie_dispatch<30, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29);
        }
    };

    template <class T>
    struct tie_dispatch<31, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30);
        }
    };

    template <class T>
    struct tie_dispatch<32, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31);
        }
    };

    template <class T>
    struct tie_dispatch<33, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32);
        }
    };

    template <class T>
    struct tie_dispatch<34, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33);
        }
    };

    template <class T>
    struct tie_dispatch<35, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34);
        }
    };

    template <class T>
    struct tie_dispatch<36, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35);
        }
    };

    template <class T>
    struct tie_dispatch<37, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36);
        }
    };

    template <class T>
    struct tie_dispatch<38, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37);
        }
    };

    template <class T>
    struct tie_dispatch<39, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38);
        }
    };

    template <class T>
    struct tie_dispatch<40, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39);
        }
    };

    template <class T>
    struct tie_dispatch<41, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40);
        }
    };

    template <class T>
    struct tie_dispatch<42, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41);
        }
    };

    template <class T>
    struct tie_dispatch<43, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42);
        }
    };

    template <class T>
    struct tie_dispatch<44, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43);
        }
    };

    template <class T>
    struct tie_dispatch<45, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44);
        }
    };

    template <class T>
    struct tie_dispatch<46, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45);
        }
    };

    template <class T>
    struct tie_dispatch<47, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46);
        }
    };

    template <class T>
    struct tie_dispatch<48, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47);
        }
    };

    template <class T>
    struct tie_dispatch<49, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48);
        }
    };

    template <class T>
    struct tie_dispatch<50, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49);
        }
    };

    template <class T>
    struct tie_dispatch<51, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50);
        }
    };

    template <class T>
    struct tie_dispatch<52, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51);
        }
    };

    template <class T>
    struct tie_dispatch<53, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                    ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,
                    p52] =
                v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52);
        }
    };

    template <class T>
    struct tie_dispatch<54, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,
                p53] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53);
        }
    };

    template <class T>
    struct tie_dispatch<55, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54);
        }
    };

    template <class T>
    struct tie_dispatch<56, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55);
        }
    };

    template <class T>
    struct tie_dispatch<57, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56);
        }
    };

    template <class T>
    struct tie_dispatch<58, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57);
        }
    };

    template <class T>
    struct tie_dispatch<59, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58);
        }
    };

    template <class T>
    struct tie_dispatch<60, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59);
        }
    };

    template <class T>
    struct tie_dispatch<61, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60);
        }
    };

    template <class T>
    struct tie_dispatch<62, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61);
        }
    };

    template <class T>
    struct tie_dispatch<63, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62);
        }
    };

    template <class T>
    struct tie_dispatch<64, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63);
        }
    };

    template <class T>
    struct tie_dispatch<65, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64);
        }
    };

    template <class T>
    struct tie_dispatch<66, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65);
        }
    };

    template <class T>
    struct tie_dispatch<67, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66);
        }
    };

    template <class T>
    struct tie_dispatch<68, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67);
        }
    };

    template <class T>
    struct tie_dispatch<69, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68);
        }
    };

    template <class T>
    struct tie_dispatch<70, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69);
        }
    };

    template <class T>
    struct tie_dispatch<71, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70);
        }
    };

    template <class T>
    struct tie_dispatch<72, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71);
        }
    };

    template <class T>
    struct tie_dispatch<73, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72);
        }
    };

    template <class T>
    struct tie_dispatch<74, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73);
        }
    };

    template <class T>
    struct tie_dispatch<75, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74);
        }
    };

    template <class T>
    struct tie_dispatch<76, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75);
        }
    };

    template <class T>
    struct tie_dispatch<77, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76);
        }
    };

    template <class T>
    struct tie_dispatch<78, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77);
        }
    };

    template <class T>
    struct tie_dispatch<79, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                    ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52
                    ,p53
                    ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,
                    p78] =
                v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78);
        }
    };

    template <class T>
    struct tie_dispatch<80, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,
                p79] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79);
        }
    };

    template <class T>
    struct tie_dispatch<81, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80);
        }
    };

    template <class T>
    struct tie_dispatch<82, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81);
        }
    };

    template <class T>
    struct tie_dispatch<83, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82);
        }
    };

    template <class T>
    struct tie_dispatch<84, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83);
        }
    };

    template <class T>
    struct tie_dispatch<85, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84);
        }
    };

    template <class T>
    struct tie_dispatch<86, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85);
        }
    };

    template <class T>
    struct tie_dispatch<87, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86);
        }
    };

    template <class T>
    struct tie_dispatch<88, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87);
        }
    };

    template <class T>
    struct tie_dispatch<89, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88);
        }
    };

    template <class T>
    struct tie_dispatch<90, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89);
        }
    };

    template <class T>
    struct tie_dispatch<91, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90);
        }
    };

    template <class T>
    struct tie_dispatch<92, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91);
        }
    };

    template <class T>
    struct tie_dispatch<93, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92);
        }
    };

    template <class T>
    struct tie_dispatch<94, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93);
        }
    };

    template <class T>
    struct tie_dispatch<95, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94);
        }
    };

    template <class T>
    struct tie_dispatch<96, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95);
        }
    };

    template <class T>
    struct tie_dispatch<97, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96);
        }
    };

    template <class T>
    struct tie_dispatch<98, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97);
        }
    };

    template <class T>
    struct tie_dispatch<99, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98);
        }
    };

    template <class T>
    struct tie_dispatch<100, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99);
        }
    };

    template <class T>
    struct tie_dispatch<101, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100);
        }
    };

    template <class T>
    struct tie_dispatch<102, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101);
        }
    };

    template <class T>
    struct tie_dispatch<103, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102);
        }
    };

    template <class T>
    struct tie_dispatch<104, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                    ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52
                    ,p53
                    ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78
                    ,p79
                    ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,
                    p103] =
                v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103);
        }
    };

    template <class T>
    struct tie_dispatch<105, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104);
        }
    };

    template <class T>
    struct tie_dispatch<106, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105);
        }
    };

    template <class T>
    struct tie_dispatch<107, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106);
        }
    };

    template <class T>
    struct tie_dispatch<108, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107);
        }
    };

    template <class T>
    struct tie_dispatch<109, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108);
        }
    };

    template <class T>
    struct tie_dispatch<110, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109);
        }
    };

    template <class T>
    struct tie_dispatch<111, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110);
        }
    };

    template <class T>
    struct tie_dispatch<112, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111);
        }
    };

    template <class T>
    struct tie_dispatch<113, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112);
        }
    };

    template <class T>
    struct tie_dispatch<114, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113);
        }
    };

    template <class T>
    struct tie_dispatch<115, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114);
        }
    };

    template <class T>
    struct tie_dispatch<116, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114,p115] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115);
        }
    };

    template <class T>
    struct tie_dispatch<117, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114,p115,p116] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115, p116);
        }
    };

    template <class T>
    struct tie_dispatch<118, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114,p115,p116,p117] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115, p116,
                                   p117);
        }
    };

    template <class T>
    struct tie_dispatch<119, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114,p115,p116,p117,p118] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115, p116,
                                   p117, p118);
        }
    };

    template <class T>
    struct tie_dispatch<120, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114,p115,p116,p117,p118,p119] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115, p116,
                                   p117, p118, p119);
        }
    };

    template <class T>
    struct tie_dispatch<121, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114,p115,p116,p117,p118,p119,p120] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115, p116,
                                   p117, p118, p119, p120);
        }
    };

    template <class T>
    struct tie_dispatch<122, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114,p115,p116,p117,p118,p119,p120,p121] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115, p116,
                                   p117, p118, p119, p120, p121);
        }
    };

    template <class T>
    struct tie_dispatch<123, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114,p115,p116,p117,p118,p119,p120,p121,p122] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115, p116,
                                   p117, p118, p119, p120, p121, p122);
        }
    };

    template <class T>
    struct tie_dispatch<124, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                    ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52
                    ,p53
                    ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78
                    ,p79
                    ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103
                    ,
                    p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114,p115,p116,p117,p118,p119,p120,p121,p122,p123]
                =
                v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115, p116,
                                   p117, p118, p119, p120, p121, p122, p123);
        }
    };

    template <class T>
    struct tie_dispatch<125, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114,p115,p116,p117,p118,p119,p120,p121,p122,p123,
                p124] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115, p116,
                                   p117, p118, p119, p120, p121, p122, p123, p124);
        }
    };

    template <class T>
    struct tie_dispatch<126, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114,p115,p116,p117,p118,p119,p120,p121,p122,p123,p124
                ,p125] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115, p116,
                                   p117, p118, p119, p120, p121, p122, p123, p124, p125);
        }
    };

    template <class T>
    struct tie_dispatch<127, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114,p115,p116,p117,p118,p119,p120,p121,p122,p123,p124
                ,p125,p126] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115, p116,
                                   p117, p118, p119, p120, p121, p122, p123, p124, p125, p126);
        }
    };

    template <class T>
    struct tie_dispatch<128, T>
    {
        static constexpr auto apply(T& v)
        {
            auto& [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27
                ,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53
                ,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66,p67,p68,p69,p70,p71,p72,p73,p74,p75,p76,p77,p78,p79
                ,p80,p81,p82,p83,p84,p85,p86,p87,p88,p89,p90,p91,p92,p93,p94,p95,p96,p97,p98,p99,p100,p101,p102,p103,
                p104,p105,p106,p107,p108,p109,p110,p111,p112,p113,p114,p115,p116,p117,p118,p119,p120,p121,p122,p123,p124
                ,p125,p126,p127] = v;
            return ::ureflect::tie(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18,
                                   p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35,
                                   p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52,
                                   p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, p64, p65, p66, p67, p68, p69,
                                   p70, p71, p72, p73, p74, p75, p76, p77, p78, p79, p80, p81, p82, p83, p84, p85, p86,
                                   p87, p88, p89, p90, p91, p92, p93, p94, p95, p96, p97, p98, p99, p100, p101, p102,
                                   p103, p104, p105, p106, p107, p108, p109, p110, p111, p112, p113, p114, p115, p116,
                                   p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127);
        }
    };

    template <class T, std::size_t N = count_members<T>>
        requires(N <= 128)
    constexpr auto to_tie(T& v) { return tie_dispatch<N, T>::apply(v); }

    template <class T, std::size_t N = count_members<T>>
        requires(N <= 128)
    constexpr auto to_tie(const T& v) { return ::ureflect::to_tie(const_cast<T&>(v)); }

    template <class T>
    extern const T uref_external;

    template <class T>
    struct ptr_box
    {
        const T* p;
    };

    template <std::size_t N, class T>
    consteval auto get_ptr(T&& t)
    {
        auto tiev = ::ureflect::to_tie(t);
        return ::ureflect::ptr_box<std::remove_reference_t<decltype(::ureflect::get<N>(tiev))>>{
            &::ureflect::get<N>(tiev)
        };
    }

    template <auto X>
    consteval std::string_view mangled_name() { return UREFL_PRETTY; }

    template <class T>
    consteval std::string_view mangled_name() { return UREFL_PRETTY; }

    struct UREFLECT_REFLECTOR
    {
        int UREFLECT_FIELD;
    };

    struct reflect_field
    {
        static constexpr std::string_view name =
            ::ureflect::mangled_name<&UREFLECT_REFLECTOR::UREFLECT_FIELD>();
        static constexpr std::size_t pos = ::ureflect::sv_find_token(name, "UREFLECT_FIELD");
        static constexpr char sep_before = (pos > 0) ? name[pos - 1] : ':';
        static constexpr char tail_after = (pos != std::string_view::npos &&
                                               pos + sizeof("UREFLECT_FIELD") - 1 < name.size())
                                               ? name[pos + sizeof("UREFLECT_FIELD") - 1]
                                               : ']';
    };

    struct reflect_type
    {
        static constexpr std::string_view name = ::ureflect::mangled_name<UREFLECT_REFLECTOR>();
#if defined(__GNUC__) || defined(__clang__)
        static constexpr std::string_view begin = "T = ";
#endif
    };

    template <std::size_t N, class T>
    struct member_nameof_impl
    {
        static constexpr std::string_view raw =
            ::ureflect::mangled_name<
                ::ureflect::get_ptr<N>(::ureflect::uref_external<std::remove_volatile_t<T>>)
            >();

        static consteval std::string_view compute()
        {
            using namespace ::ureflect;
            constexpr std::string_view m1 = "&uref_external.";
            std::size_t k = sv_find_token(raw, m1);
            if (k != std::string_view::npos)
            {
                const std::size_t b = k + m1.size();
                const std::size_t e = sv_find_first_of(raw, std::string_view("}]>), ;"), b);
                return raw.substr(b, (e == std::string_view::npos ? raw.size() : e) - b);
            }

            constexpr std::string_view m2 = "uref_external.";
            k = sv_find_token(raw, m2);
            if (k != std::string_view::npos)
            {
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

    template <std::size_t N, class T>
    inline constexpr std::string_view member_nameof = member_nameof_impl<N, T>::value;


    template <class T, std::size_t... I>
    consteval auto member_names_impl(std::index_sequence<I...>)
    {
        if constexpr (sizeof...(I) == 0) return std::array<std::string_view, 0>{};
        else return std::array{::ureflect::member_nameof<I, T>...};
    }

    template <class T>
    inline constexpr auto member_names =
        member_names_impl<T>(std::make_index_sequence<count_members<T>>{});

    template <class T, class F>
    constexpr void for_each_field(T& obj, F&& fn)
    {
        constexpr std::size_t N = count_members<T>;
        if constexpr (N == 0) return;
        auto tiev = ::ureflect::to_tie(obj);
        [&]<std::size_t... I>(std::index_sequence<I...>)
        {
            (fn(::ureflect::member_nameof<I, T>, ::ureflect::get<I>(tiev)), ...);
        }(std::make_index_sequence<N>{});
    }
} // namespace ureflect

#endif
