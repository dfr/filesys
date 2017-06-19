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

// -*- c++ -*-
#pragma once

#include <cstring>
#include <map>
#include <mutex>
#include <keyval/keyval.h>

namespace keyval {
namespace memory {

class BufferCompare
{
public:
    int operator()(const std::shared_ptr<Buffer>& x,
                   const std::shared_ptr<Buffer>& y) const
    {
        auto xsz = x->size(), ysz = y->size();
        auto sz = std::min(xsz, ysz);
        auto cmp = ::memcmp(x->data(), y->data(), sz);
        if (cmp) return cmp < 0;
        return xsz < ysz;
    }
};

typedef std::map<std::shared_ptr<Buffer>,
                 std::shared_ptr<Buffer>,
                 BufferCompare> namespaceT;

class MemoryNamespace;

class MemoryDatabase: public Database
{
public:
    MemoryDatabase();

    // Database overrides
    std::shared_ptr<Namespace> getNamespace(const std::string& name) override;
    std::unique_ptr<Transaction> beginTransaction() override;
    void commit(std::unique_ptr<Transaction>&& transaction) override;
    void flush() override {}
    bool isReplicated() override { return false; }
    bool isMaster() override { return true; }
    bool get(
        std::shared_ptr<oncrpc::RestRequest> req,
        std::unique_ptr<oncrpc::RestEncoder>&& res) override { return false; }
    void onMasterChange(std::function<void(bool)> cb) override {}
    void setAppData(const std::vector<uint8_t>& data) override {}
    std::vector<ReplicaInfo> getReplicas() override { return {}; }

private:
    std::mutex mutex_;
    std::map<std::string, std::shared_ptr<MemoryNamespace>> namespaces_;
};

class MemoryNamespace: public Namespace
{
public:
    MemoryNamespace(std::mutex& mutex)
        : mutex_(mutex)
    {
    }

    auto lock()
    {
        return std::unique_lock<std::mutex>(mutex_);
    }

    std::unique_ptr<Iterator> iterator() override;
    std::unique_ptr<Iterator> iterator(
        std::shared_ptr<Buffer> startKey, std::shared_ptr<Buffer> endKey) override;
    std::shared_ptr<Buffer> get(std::shared_ptr<Buffer> key) override;
    std::uint64_t spaceUsed(
        std::shared_ptr<Buffer> start, std::shared_ptr<Buffer> end) override;

    auto& map() { return map_; }
    auto gen() const { return gen_; }

    // These are only called from MemoryTransaction::commit with the
    // mutex locked
    void put(std::shared_ptr<Buffer> key, std::shared_ptr<Buffer> value);
    void remove(std::shared_ptr<Buffer> key);

private:
    std::mutex& mutex_;
    std::uint64_t gen_ = 1;
    namespaceT map_;
};

class MemoryIterator: public Iterator
{
public:
    MemoryIterator(MemoryNamespace& ns)
        : ns_(ns)
    {
        auto lk = ns.lock();
        gen_ = ns.gen();
        it_ = ns.map().begin();
        read();
    }

    MemoryIterator(
        MemoryNamespace& ns,
        std::shared_ptr<Buffer> startKey,
        std::shared_ptr<Buffer> endKey)
        : ns_(ns),
          endKey_(endKey)
    {
        auto lk = ns.lock();
        gen_ = ns.gen();
        it_ = ns.map().lower_bound(startKey);
        read();
    }

    // Read an entry from the map iterator. Must be called with the
    // mutex locked.
    void read()
    {
        valid_ = it_ != ns_.map().end();
        if (valid_) {
            key_ = it_->first;
            value_ = it_->second;
        }
        else {
            key_.reset();
            value_.reset();
        }
    }

    void seek(std::shared_ptr<Buffer> key) override;
    void seekToFirst() override;
    void seekToLast() override;
    void next() override;
    void prev() override;
    bool valid() const override;
    std::shared_ptr<Buffer> key() const override;
    std::shared_ptr<Buffer> value() const override;

private:
    MemoryNamespace& ns_;
    std::uint64_t gen_;
    namespaceT::const_iterator it_;
    bool valid_;
    std::shared_ptr<Buffer> endKey_;
    std::shared_ptr<Buffer> key_;
    std::shared_ptr<Buffer> value_;
};

class MemoryTransaction: public Transaction
{
public:
    void put(
        std::shared_ptr<Namespace> ns,
        std::shared_ptr<Buffer> key,
        std::shared_ptr<Buffer> val) override;
    void remove(
        std::shared_ptr<Namespace> ns,
        std::shared_ptr<Buffer> key) override;

    void commit();

private:
    std::vector<std::function<void()>> ops_;
};

}
}
