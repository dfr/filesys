/*-
 * Copyright (c) 2016-present Doug Rabson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
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
