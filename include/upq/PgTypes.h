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

#include <libpq-fe.h>

#include "uvent/Uvent.h"
#include "uvent/utils/sync/RefCountedSession.h"

namespace usub::pg
{
    enum class PgErrorCode : uint32_t
    {
        OK = 0,
        InvalidFuture,
        ConnectionClosed,
        SocketReadFailed,
        ProtocolCorrupt,
        ParserTruncatedField,
        ParserTruncatedRow,
        ParserTruncatedHeader,
        ServerError,
        AuthFailed,
        AwaitCanceled,
        Unknown
    };

    // High-level server-side error classification, derived from SQLSTATE.
    enum class PgSqlStateClass : uint8_t
    {
        None = 0, // no sqlstate / not an error
        ConnectionError, // 08***, connection failures etc.
        SyntaxError, // 42*** (syntax/access rule)
        UndefinedObject, // e.g. 42P01 (relation does not exist)
        ConstraintViolation, // 23*** generic integrity violation
        UniqueViolation, // 23505
        CheckViolation, // 23514
        NotNullViolation, // 23502
        ForeignKeyViolation, // 23503
        Deadlock, // 40P01
        SerializationFailure, // 40001
        PrivilegeError, // 42501 / 28***
        DataException, // 22***
        TransactionState, // 25*** / 40*** rollback
        InternalError, // XX***
        Other // fallback
    };

    inline const char* toString(PgErrorCode code) noexcept
    {
        switch (code)
        {
        case PgErrorCode::OK: return "OK";
        case PgErrorCode::InvalidFuture: return "InvalidFuture";
        case PgErrorCode::ConnectionClosed: return "ConnectionClosed";
        case PgErrorCode::SocketReadFailed: return "SocketReadFailed";
        case PgErrorCode::ProtocolCorrupt: return "ProtocolCorrupt";
        case PgErrorCode::ParserTruncatedField: return "ParserTruncatedField";
        case PgErrorCode::ParserTruncatedRow: return "ParserTruncatedRow";
        case PgErrorCode::ParserTruncatedHeader: return "ParserTruncatedHeader";
        case PgErrorCode::ServerError: return "ServerError";
        case PgErrorCode::AuthFailed: return "AuthFailed";
        case PgErrorCode::AwaitCanceled: return "AwaitCanceled";
        case PgErrorCode::Unknown: return "Unknown";
        }
        return "InvalidPgErrorCode";
    }

    inline const char* toString(PgSqlStateClass cls) noexcept
    {
        switch (cls)
        {
        case PgSqlStateClass::None: return "None";
        case PgSqlStateClass::ConnectionError: return "ConnectionError";
        case PgSqlStateClass::SyntaxError: return "SyntaxError";
        case PgSqlStateClass::UndefinedObject: return "UndefinedObject";
        case PgSqlStateClass::ConstraintViolation: return "ConstraintViolation";
        case PgSqlStateClass::UniqueViolation: return "UniqueViolation";
        case PgSqlStateClass::CheckViolation: return "CheckViolation";
        case PgSqlStateClass::NotNullViolation: return "NotNullViolation";
        case PgSqlStateClass::ForeignKeyViolation: return "ForeignKeyViolation";
        case PgSqlStateClass::Deadlock: return "Deadlock";
        case PgSqlStateClass::SerializationFailure: return "SerializationFailure";
        case PgSqlStateClass::PrivilegeError: return "PrivilegeError";
        case PgSqlStateClass::DataException: return "DataException";
        case PgSqlStateClass::TransactionState: return "TransactionState";
        case PgSqlStateClass::InternalError: return "InternalError";
        case PgSqlStateClass::Other: return "Other";
        }
        return "InvalidPgSqlStateClass";
    }

    struct PgErrorDetail
    {
        std::string sqlstate;
        std::string message;
        std::string detail;
        std::string hint;
        PgSqlStateClass category{PgSqlStateClass::None};
    };

    struct QueryResult
    {
        struct Row
        {
            std::vector<std::string> cols;

            const std::string& operator[](size_t i) const noexcept
            {
                return cols[i];
            }

            std::string& operator[](size_t i) noexcept
            {
                return cols[i];
            }

            [[nodiscard]] inline size_t size() const noexcept { return this->cols.size(); }
            [[nodiscard]] inline bool empty() const noexcept { return this->cols.empty(); }
        };

        std::vector<Row> rows;

        const Row& operator[](size_t i) const noexcept
        {
            return this->rows[i];
        }

        Row& operator[](size_t i) noexcept
        {
            return this->rows[i];
        }

        bool ok{false};
        PgErrorCode code{PgErrorCode::Unknown};

        std::string error;

        PgErrorDetail err_detail;

        bool rows_valid{true};

        [[nodiscard]] inline bool empty() const noexcept { return this->ok && this->rows_valid && this->rows.empty(); }

        [[nodiscard]] inline bool has_rows() const noexcept
        {
            return this->ok && this->rows_valid && !this->rows.empty();
        }

        [[nodiscard]] inline size_t row_count() const noexcept { return this->rows.size(); }

        [[nodiscard]] inline size_t col_count() const noexcept
        {
            return this->rows.empty() ? 0 : this->rows[0].cols.size();
        }

        [[nodiscard]] inline bool invariant() const noexcept
        {
            return this->rows.empty() || !this->rows[0].cols.empty();
        }
    };

    class QueryState : public utils::sync::refc::RefCounted<QueryState>
    {
    public:
        std::atomic<bool> ready{false};
        QueryResult result;

        std::string sql;

        std::coroutine_handle<> awaiting_coro{nullptr};

        std::atomic<bool> canceled{false};
        PgErrorCode cancel_code{PgErrorCode::OK};
        std::string cancel_reason;
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

    struct PgWireError
    {
        PgErrorCode code{PgErrorCode::Unknown};
        std::string message;
    };

    template <class T>
    struct PgWireResult
    {
        T value{};
        bool ok{false};
        PgWireError err;
    };

    template <>
    struct PgWireResult<void>
    {
        bool ok{false};
        PgWireError err;
    };

    // classify SQLSTATE string ("23505", "40P01", etc.) into PgSqlStateClass
    PgSqlStateClass classify_sqlstate(std::string_view sqlstate);

    template <class Socket>
    uvent::task::Awaitable<PgWireResult<void>>
    read_exact(Socket& sock, std::vector<uint8_t>& buf, size_t n)
    {
        PgWireResult<void> out;
        out.ok = false;
        out.err.code = PgErrorCode::Unknown;
        out.err.message.clear();

        buf.resize(n);
        size_t off = 0;

        while (off < n)
        {
            uvent::utils::DynamicBuffer tmp;
            tmp.reserve(n - off);

            ssize_t r = co_await sock.async_read(tmp, n - off);
            if (r <= 0)
            {
                out.err.code = PgErrorCode::SocketReadFailed;
                out.err.message = "async_read returned <=0";
                co_return out;
            }

            std::memcpy(buf.data() + off, tmp.data(), (size_t)r);
            off += (size_t)r;
        }

        out.ok = true;
        co_return out;
    }

    template <class Socket>
    uvent::task::Awaitable<PgWireResult<PgFrame>>
    read_frame(Socket& sock)
    {
        PgWireResult<PgFrame> out;
        out.ok = false;
        out.err.code = PgErrorCode::Unknown;
        out.err.message.clear();

        std::vector<uint8_t> header;
        {
            auto hdr_res = co_await read_exact(sock, header, 5);
            if (!hdr_res.ok)
            {
                out.err = hdr_res.err;
                co_return out;
            }
        }

        char type = (char)header[0];
        uint32_t len = read_be32(&header[1]);

        if (len < 4)
        {
            out.err.code = PgErrorCode::ProtocolCorrupt;
            out.err.message = "frame length < 4";
            co_return out;
        }

        uint32_t payload_len = len - 4;

        std::vector<uint8_t> payload;
        {
            auto pay_res = co_await read_exact(sock, payload, payload_len);
            if (!pay_res.ok)
            {
                out.err = pay_res.err;
                co_return out;
            }
        }

        out.value.type = type;
        out.value.payload = std::move(payload);
        out.ok = true;
        co_return out;
    }

    struct PgServerErrorFields
    {
        std::string severity;
        std::string sqlstate;
        std::string message;
        std::string detail;
        std::string hint;
    };

    PgServerErrorFields parse_error_fields(const std::vector<uint8_t>& payload);

    std::string parse_error(const std::vector<uint8_t>& payload);

    struct RowParseContext
    {
        bool ok{true};
        PgErrorCode code{PgErrorCode::OK};
        std::string msg;
    };

    void parse_row_description_ex(const std::vector<uint8_t>& payload,
                                  std::vector<std::string>& out_cols,
                                  RowParseContext& ctx);

    inline void parse_row_description(const std::vector<uint8_t>& payload,
                                      std::vector<std::string>& out_cols)
    {
        RowParseContext ctx;
        parse_row_description_ex(payload, out_cols, ctx);
    }

    void parse_data_row_ex(const std::vector<uint8_t>& payload,
                           QueryResult::Row& out_row,
                           RowParseContext& ctx);

    inline void parse_data_row(const std::vector<uint8_t>& payload,
                               QueryResult::Row& out_row)
    {
        RowParseContext ctx;
        parse_data_row_ex(payload, out_row, ctx);
    }

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

    struct PgCopyResult
    {
        bool ok{false};
        PgErrorCode code{PgErrorCode::Unknown};
        std::string error;
        PgErrorDetail err_detail;
        uint64_t rows_affected{0};
    };

    struct PgCursorChunk
    {
        std::vector<QueryResult::Row> rows;
        bool done{false};
        bool ok{false};
        PgErrorCode code{PgErrorCode::Unknown};
        std::string error;
        PgErrorDetail err_detail;
    };

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
} // namespace usub::pg

#endif
