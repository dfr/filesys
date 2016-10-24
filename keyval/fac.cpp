/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <keyval/keyval.h>

#include "keyval/mem/mem.h"
#include "keyval/rocks/rocks.h"
#include "keyval/paxos/paxos.h"

using namespace keyval;

std::shared_ptr<Database> keyval::make_memdb()
{
    return std::make_shared<keyval::memory::MemoryDatabase>();
}

std::shared_ptr<Database> keyval::make_rocksdb(const std::string& filename)
{
    return std::make_shared<keyval::rocks::RocksDatabase>(filename);
}

std::shared_ptr<Database> keyval::make_paxosdb(
    const std::string& filename,
    const std::string& addr,
    const std::vector<std::string>& replicas,
    std::shared_ptr<oncrpc::SocketManager> sockman)
{
    auto clock = std::make_shared<util::SystemClock>();
    auto db = std::make_shared<keyval::rocks::RocksDatabase>(filename);
    return std::make_shared<keyval::paxos::KVReplica>(
        addr, replicas, clock, sockman, db);
}
