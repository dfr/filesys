/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
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
    std::vector<std::vector<uint8_t>> getAppData() override { return {}; }

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
    std::unique_ptr<Iterator> iterator(std::shared_ptr<Buffer> key) override;
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

    MemoryIterator(MemoryNamespace& ns, std::shared_ptr<Buffer> key)
        : ns_(ns)
    {
        auto lk = ns.lock();
        gen_ = ns.gen();
        it_ = ns.map().lower_bound(key);
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
    bool valid(std::shared_ptr<Buffer> endKey) const override;
    std::shared_ptr<Buffer> key() const override;
    std::shared_ptr<Buffer> value() const override;

private:
    MemoryNamespace& ns_;
    std::uint64_t gen_;
    namespaceT::const_iterator it_;
    bool valid_;
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
