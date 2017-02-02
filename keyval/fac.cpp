/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#include <keyval/keyval.h>

#include "keyval/mem/mem.h"
#include "keyval/rocks/rocks.h"
#include "keyval/paxos/paxos.h"

#include <rpc++/sockman.h>

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
    const std::vector<std::string>& replicas)
{
    auto clock = std::make_shared<util::SystemClock>();
    auto db = std::make_shared<keyval::rocks::RocksDatabase>(filename);
    auto sockman = std::make_shared<oncrpc::SocketManager>();
    return std::make_shared<keyval::paxos::KVReplica>(
        replicas, clock, sockman, db);
}
