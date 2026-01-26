//
// Created by kirill on 1/26/26.
//

#ifndef BACKEND_NOTIFICATION_HANDLER_CONNINFO_H
#define BACKEND_NOTIFICATION_HANDLER_CONNINFO_H

#include <upq/PgConnection.h>
#include <upq/utils/IPAddressUtils.h>

namespace usub::pg {
    static inline std::expected<std::string, utils::ConninfoError> make_conninfo(
        std::string_view host, std::string_view port, std::string_view user,
        std::string_view dbname, std::string_view password, const SSLConfig &ssl) {
        using utils::escape_conninfo_value;
        using utils::is_ip_literal;
        using utils::strip_brackets;

        auto add_kv = [](std::string &out, std::string_view k, const std::string &ev) {
            if (!out.empty()) out.push_back(' ');
            out.append(k);
            out.push_back('=');
            out.append(ev);
        };

        std::string ci;
        ci.reserve(256);

        const std::string_view host_raw = strip_brackets(host);
        const bool host_is_ip = is_ip_literal(host_raw);

        if (ssl.server_hostname && host_is_ip) {
            auto ev_addr = escape_conninfo_value(host_raw);
            if (!ev_addr) return std::unexpected(ev_addr.error());
            add_kv(ci, "hostaddr", *ev_addr);

            auto ev_hn = escape_conninfo_value(*ssl.server_hostname);
            if (!ev_hn) return std::unexpected(ev_hn.error());
            add_kv(ci, "host", *ev_hn);
        } else if (ssl.server_hostname) {
            auto ev_hn = escape_conninfo_value(*ssl.server_hostname);
            if (!ev_hn) return std::unexpected(ev_hn.error());
            add_kv(ci, "host", *ev_hn);
        } else {
            auto ev_host = escape_conninfo_value(host_raw);
            if (!ev_host) return std::unexpected(ev_host.error());
            add_kv(ci, "host", *ev_host);
        }

        {
            auto ev = escape_conninfo_value(port);
            if (!ev) return std::unexpected(ev.error());
            add_kv(ci, "port", *ev);
        }
        {
            auto ev = escape_conninfo_value(user);
            if (!ev) return std::unexpected(ev.error());
            add_kv(ci, "user", *ev);
        }
        {
            auto ev = escape_conninfo_value(dbname);
            if (!ev) return std::unexpected(ev.error());
            add_kv(ci, "dbname", *ev);
        }
        {
            auto ev = escape_conninfo_value(password);
            if (!ev) return std::unexpected(ev.error());
            add_kv(ci, "password", *ev);
        }

        {
            auto ev = escape_conninfo_value(to_string(ssl.mode));
            if (!ev) return std::unexpected(ev.error());
            add_kv(ci, "sslmode", *ev);
        }

        if (ssl.root_cert) {
            auto ev = escape_conninfo_value(*ssl.root_cert);
            if (!ev) return std::unexpected(ev.error());
            add_kv(ci, "sslrootcert", *ev);
        }

        if (ssl.client_cert) {
            auto ev = escape_conninfo_value(*ssl.client_cert);
            if (!ev) return std::unexpected(ev.error());
            add_kv(ci, "sslcert", *ev);
        }

        if (ssl.client_key) {
            auto ev = escape_conninfo_value(*ssl.client_key);
            if (!ev) return std::unexpected(ev.error());
            add_kv(ci, "sslkey", *ev);
        }

        if (ssl.crl) {
            auto ev = escape_conninfo_value(*ssl.crl);
            if (!ev) return std::unexpected(ev.error());
            add_kv(ci, "sslcrl", *ev);
        }

        return ci;
    }
}  // namespace usub::pg

#endif  // BACKEND_NOTIFICATION_HANDLER_CONNINFO_H
