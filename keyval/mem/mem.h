// -*- c++ -*-
#pragma once

#include <map>
#include <kv++/keyval.h>

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

private:
    std::map<std::string, std::shared_ptr<MemoryNamespace>> namespaces_;
};

class MemoryNamespace: public Namespace
{
public:
    std::unique_ptr<Iterator> iterator() override;
    std::unique_ptr<Iterator> iterator(std::shared_ptr<Buffer> key) override;
    std::shared_ptr<Buffer> get(std::shared_ptr<Buffer> key) override;
    std::uint64_t spaceUsed(
        std::shared_ptr<Buffer> start, std::shared_ptr<Buffer> end) override;

    namespaceT& map() { return map_; }

private:
    namespaceT map_;
};

class MemoryIterator: public Iterator
{
public:
    MemoryIterator(const namespaceT& map)
        : map_(map),
          it_(map.begin())
    {}
    MemoryIterator(const namespaceT& map, std::shared_ptr<Buffer> key)
        : map_(map),
          it_(map_.lower_bound(key))
    {}
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
    const namespaceT& map_;
    namespaceT::const_iterator it_;
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
