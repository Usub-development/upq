#include "upq/PgTypes.h"

namespace usub::pg
{
    void QueryState::set_result(QueryResult&& r)
    {
        {
            std::lock_guard lk(mtx);
            result = std::move(r);
            ready.store(true, std::memory_order_release);
        }

        cv.notify_all();

        if (this->awaiting_coro)
        {
            auto h = this->awaiting_coro;
            this->awaiting_coro = nullptr;
            h.resume();
        }
    }

    QueryResult QueryFuture::wait()
    {
        if (!this->state)
        {
            QueryResult bad;
            bad.ok = false;
            bad.error = "invalid future";
            return bad;
        }

        if (!this->state->ready.load(std::memory_order_acquire))
        {
            std::unique_lock lk(this->state->mtx);
            this->state->cv.wait(lk, [&]
            {
                return this->state->ready.load(std::memory_order_acquire);
            });
        }

        return this->state->result;
    }

    void write_be32(uint8_t* dst, uint32_t v)
    {
        dst[0] = (v >> 24) & 0xFF;
        dst[1] = (v >> 16) & 0xFF;
        dst[2] = (v >> 8) & 0xFF;
        dst[3] = (v >> 0) & 0xFF;
    }

    uint32_t read_be32(const uint8_t* src)
    {
        return (uint32_t(src[0]) << 24) |
            (uint32_t(src[1]) << 16) |
            (uint32_t(src[2]) << 8) |
            (uint32_t(src[3]) << 0);
    }

    std::string parse_error(const std::vector<uint8_t>& payload)
    {
        std::string msg;
        size_t i = 0;
        while (i < payload.size())
        {
            uint8_t code = payload[i++];
            if (code == 0) break;

            const char* start = (const char*)&payload[i];
            size_t len = std::strlen(start);

            if (!msg.empty()) msg += " | ";
            msg += std::string(1, (char)code);
            msg += ": ";
            msg += std::string(start, len);

            i += len + 1;
        }

        return msg;
    }

    void parse_row_description(const std::vector<uint8_t>& payload,
                               std::vector<std::string>& out_cols)
    {
        out_cols.clear();

        auto rd16 = [&](size_t ofs) -> uint16_t
        {
            return (uint16_t(payload[ofs]) << 8) | uint16_t(payload[ofs + 1]);
        };

        size_t off = 0;
        if (payload.size() < 2) return;

        uint16_t nfields = rd16(off);
        off += 2;

        out_cols.reserve(nfields);

        for (uint16_t i = 0; i < nfields; i++)
        {
            size_t start = off;
            while (off < payload.size() && payload[off] != 0) off++;

            std::string name;
            if (off < payload.size())
            {
                name.assign((const char*)&payload[start], off - start);
                off++; // skip 0
            }
            else
            {
                break;
            }

            if (off + 18 > payload.size()) break;
            off += 18;

            out_cols.push_back(std::move(name));
        }

    }

    void parse_data_row(const std::vector<uint8_t>& payload,
                        QueryResult::Row& out_row)
    {
        auto rd16 = [&](size_t ofs) -> uint16_t
        {
            return (uint16_t(payload[ofs]) << 8) | uint16_t(payload[ofs + 1]);
        };
        auto rd32 = [&](size_t ofs) -> uint32_t
        {
            return read_be32(&payload[ofs]);
        };

        size_t off = 0;
        if (payload.size() < 2) return;

        uint16_t ncols = rd16(off);
        off += 2;
        out_row.cols.reserve(ncols);

        for (uint16_t c = 0; c < ncols; c++)
        {
            if (off + 4 > payload.size())
            {
                out_row.cols.emplace_back();
                break;
            }

            int32_t col_len = (int32_t)rd32(off);
            off += 4;

            if (col_len == -1)
            {
                out_row.cols.emplace_back();
            }
            else
            {
                if (off + (size_t)col_len > payload.size())
                {
                    out_row.cols.emplace_back();
                    break;
                }
                out_row.cols.emplace_back(
                    (const char*)&payload[off],
                    (size_t)col_len
                );
                off += (size_t)col_len;
            }
        }

    }

    std::vector<uint8_t> build_startup_message(const std::string& user,
                                               const std::string& db)
    {
        auto push_cstr = [&](std::vector<uint8_t>& v, const char* s)
        {
            while (*s) { v.push_back((uint8_t)*s++); }
            v.push_back(0);
        };
        auto push_kv = [&](std::vector<uint8_t>& v,
                           const char* k,
                           const std::string& val)
        {
            push_cstr(v, k);
            for (char c : val) v.push_back((uint8_t)c);
            v.push_back(0);
        };

        std::vector<uint8_t> tail;
        push_kv(tail, "user", user);
        push_kv(tail, "database", db);
        push_kv(tail, "client_encoding", "UTF8");
        tail.push_back(0); // terminator

        uint32_t total_len = 4 /*len*/ + 4 /*protocol*/ + (uint32_t)tail.size();

        std::vector<uint8_t> out;
        out.resize(8);
        write_be32(out.data() + 0, total_len);
        write_be32(out.data() + 4, 196608); // protocol 3.0
        out.insert(out.end(), tail.begin(), tail.end());

        return out;
    }

    std::vector<uint8_t> build_password_message(const std::string& password)
    {
        uint32_t len = 4 + (uint32_t)password.size() + 1;

        std::vector<uint8_t> out;
        out.reserve(1 + 4 + password.size() + 1);

        out.push_back('p');
        uint8_t be[4];
        write_be32(be, len);
        out.insert(out.end(), be, be + 4);
        out.insert(out.end(), password.begin(), password.end());
        out.push_back(0);

        return out;
    }

    std::string md5_hex(const uint8_t* data, size_t len)
    {
        uint8_t digest[MD5_DIGEST_LENGTH];
        MD5(data, len, digest);

        static const char* hex = "0123456789abcdef";
        std::string out;
        out.resize(32);
        for (int i = 0; i < 16; i++)
        {
            out[2 * i + 0] = hex[(digest[i] >> 4) & 0xF];
            out[2 * i + 1] = hex[(digest[i]) & 0xF];
        }
        return out;
    }

    std::vector<uint8_t> build_md5_password_message(
        const std::string& user,
        const std::string& password,
        const uint8_t salt[4]
    )
    {
        // step1 = md5(password + user)
        std::string step1_src = password + user;
        std::string step1_hex = md5_hex(
            reinterpret_cast<const uint8_t*>(step1_src.data()),
            step1_src.size()
        );

        // step2 = md5(step1_hex + salt)
        std::string step2_src;
        step2_src.reserve(step1_hex.size() + 4);
        step2_src.append(step1_hex);
        step2_src.push_back((char)salt[0]);
        step2_src.push_back((char)salt[1]);
        step2_src.push_back((char)salt[2]);
        step2_src.push_back((char)salt[3]);

        std::string step2_hex = md5_hex(
            reinterpret_cast<const uint8_t*>(step2_src.data()),
            step2_src.size()
        );

        std::string final_str = "md5" + step2_hex;

        uint32_t msg_len = 4 + (uint32_t)final_str.size() + 1;
        std::vector<uint8_t> out;
        out.reserve(1 + 4 + final_str.size() + 1);

        out.push_back('p');
        uint8_t be[4];
        write_be32(be, msg_len);
        out.insert(out.end(), be, be + 4);
        out.insert(out.end(), final_str.begin(), final_str.end());
        out.push_back(0);

        return out;
    }

    void build_simple_query(std::vector<uint8_t>& out,
                            std::string_view sql)
    {
        uint32_t len = 4 + (uint32_t)sql.size() + 1;

        out.clear();
        out.reserve(1 + 4 + sql.size() + 1);

        out.push_back('Q');
        uint8_t be[4];
        write_be32(be, len);
        out.insert(out.end(), be, be + 4);
        out.insert(out.end(), sql.begin(), sql.end());
        out.push_back(0);

    }
} // namespace usub::pg
