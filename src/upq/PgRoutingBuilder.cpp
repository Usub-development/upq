//
// Created by root on 11/3/25.
//

#include <upq/PgRoutingBuilder.h>

namespace usub::pg
{
    void PgConnectorBuilder::validate()
    {
        std::unordered_set<std::string> names;
        bool has_primary = false;
        for (auto& n : this->cfg_.nodes)
        {
            if (!names.insert(n.name).second) throw std::runtime_error("duplicate node: " + n.name);
            if (n.role == NodeRole::Primary) has_primary = true;
            if (n.weight == 0) throw std::runtime_error("weight must be >0 for node: " + n.name);
        }
        if (!has_primary) throw std::runtime_error("no Primary node");
        for (auto& pf : this->cfg_.primary_failover)
            if (!names.count(pf)) throw std::runtime_error("primary_failover references unknown node: " + pf);
    }
}