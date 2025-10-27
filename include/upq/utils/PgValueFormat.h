//
// Created by root on 10/27/25.
//

#ifndef PGVALUEFORMAT_H
#define PGVALUEFORMAT_H

#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <sstream>
#include <type_traits>

namespace usub::pg
{
    template <typename T>
    std::string to_string(const T& v)
    {
        if constexpr (std::is_same_v<T, std::nullptr_t>)
            return "NULL";
        else
        {
            std::ostringstream oss;
            oss.imbue(std::locale::classic());
            oss << v;
            return oss.str();
        }
    }

    inline std::string to_string(const std::string& s) { return s; }
    inline std::string to_string(std::string_view s) { return std::string{s}; }

    inline std::string to_string(const char* s)
    {
        return s ? std::string{s} : "NULL";
    }

    template <typename T>
    std::string to_string(const std::optional<T>& v)
    {
        return v.has_value() ? to_string(*v) : "NULL";
    }

    template <typename T>
    std::string to_string(const std::vector<T>& vec)
    {
        std::string out;
        out.reserve(vec.size() * 8);
        out.push_back('{');

        bool first = true;
        for (auto&& e : vec)
        {
            if (!first) out.push_back(',');
            first = false;

            auto s = to_string(e);

            if (s == "NULL")
            {
                out += s;
                continue;
            }

            if (s.find_first_of(",{}\"\\ ") != std::string::npos)
            {
                out.push_back('"');
                for (char c : s)
                {
                    if (c == '"' || c == '\\') out.push_back('\\');
                    out.push_back(c);
                }
                out.push_back('"');
            }
            else out += s;
        }

        out.push_back('}');
        return out;
    }
}

#endif //PGVALUEFORMAT_H
