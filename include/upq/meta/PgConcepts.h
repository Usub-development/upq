//
// Created by root on 10/27/25.
//

#ifndef PGCONCEPTS_H
#define PGCONCEPTS_H

#include <string_view>
#include "uvent/Uvent.h"

namespace usub::pg
{
    template <class T>
    concept PgNotifyHandler = requires(
        T handler,
        std::string ch,
        std::string payload,
        int pid
    )
        {
            {
                handler(ch, payload, pid)
            }
            -> std::same_as<uvent::task::Awaitable<void>>;
        };

    template <class HandlerT>
        requires PgNotifyHandler<HandlerT>
    class PgNotificationListener;
}

#endif //PGCONCEPTS_H
