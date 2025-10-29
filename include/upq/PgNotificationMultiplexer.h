#ifndef PGNOTIFICATIONMULTIPLEXER_H
#define PGNOTIFICATIONMULTIPLEXER_H

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <memory>
#include <utility>
#include <concepts>

#include <libpq-fe.h>

#include "PgConnection.h"
#include "PgTypes.h"
#include "uvent/Uvent.h"

namespace usub::pg
{
    template <class HandlerT>
        requires PgNotifyHandler<HandlerT>
    class PgNotificationMultiplexer
    {
    public:
        explicit PgNotificationMultiplexer(std::shared_ptr<PgConnectionLibpq> conn)
            : conn_(std::move(conn))
        {
        }

        usub::uvent::task::Awaitable<bool>
        add_handler(std::string channel, HandlerT handler)
        {
            auto it = this->channels_.find(channel);
            if (it == this->channels_.end())
            {
                std::string listen_sql = "LISTEN " + channel + ";";
                QueryResult qr = co_await this->conn_->exec_simple_query_nonblocking(listen_sql);
                if (!qr.ok)
                {
                    co_return false;
                }

                ChannelInfo ci;
                ci.handlers.push_back(std::move(handler));
                this->channels_.emplace(std::move(channel), std::move(ci));
            }
            else
            {
                it->second.handlers.push_back(std::move(handler));
            }

            co_return true;
        }

        usub::uvent::task::Awaitable<void> run()
        {
            if (!this->conn_ || !this->conn_->connected())
            {
                co_return;
            }

            while (true)
            {
                co_await this->conn_->wait_readable_for_listener();

                PGconn* raw = this->conn_->raw_conn();
                if (!raw)
                {
                    co_return;
                }

                if (PQconsumeInput(raw) == 0)
                {
                    if (PQstatus(raw) == CONNECTION_BAD)
                    {
                        co_return;
                    }
                    continue;
                }
                while (true)
                {
                    PGnotify* n = PQnotifies(raw);
                    if (!n) break;

                    const char* ch_raw = n->relname ? n->relname : "";
                    const char* pl_raw = n->extra ? n->extra : "";
                    int be_pid = n->be_pid;

                    dispatch_async(ch_raw, pl_raw, be_pid);

                    PQfreemem(n);
                }
            }

            co_return;
        }

    private:
        struct ChannelInfo
        {
            std::vector<HandlerT> handlers;
        };

        void dispatch_async(std::string_view ch,
                            std::string_view payload,
                            int pid)
        {
            auto it = this->channels_.find(std::string(ch));
            if (it == this->channels_.end())
            {
                return;
            }

            std::string ch_copy(ch);
            std::string payload_copy(payload);
            int pid_copy = pid;

            for (auto& h : it->second.handlers)
            {
                HandlerT handler_copy = h;

                usub::uvent::system::co_spawn(
                    run_single_handler(
                        std::move(handler_copy),
                        ch_copy,
                        payload_copy,
                        pid_copy
                    )
                );
            }
        }

        static usub::uvent::task::Awaitable<void>
        run_single_handler(
            HandlerT handler_copy,
            std::string ch_copy,
            std::string payload_copy,
            int pid_copy
        )
        {
            co_await handler_copy(
                std::move(ch_copy),
                std::move(payload_copy),
                pid_copy
            );
            co_return;
        }

    private:
        std::shared_ptr<PgConnectionLibpq> conn_;
        std::unordered_map<std::string, ChannelInfo> channels_;
    };
}

#endif // PGNOTIFICATIONMULTIPLEXER_H
