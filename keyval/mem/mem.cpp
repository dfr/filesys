#include <system_error>

#include "mem.h"

using namespace keyval;
using namespace keyval::memory;
using namespace std;

MemoryDatabase::MemoryDatabase()
{
}

// Database overrides
shared_ptr<Namespace> MemoryDatabase::getNamespace(const string& name)
{
    auto it = namespaces_.find(name);
    if (it != namespaces_.end())
        return it->second;

    auto ns = make_shared<MemoryNamespace>();
    namespaces_[name] = ns;

    return ns;
}

unique_ptr<Transaction> MemoryDatabase::beginTransaction()
{
    return make_unique<MemoryTransaction>();
}

void MemoryDatabase::commit(unique_ptr<Transaction>&& transaction)
{
    auto t = reinterpret_cast<MemoryTransaction*>(transaction.get());
    t->commit();
    transaction.reset();
}

unique_ptr<keyval::Iterator> MemoryNamespace::iterator()
{
    return make_unique<MemoryIterator>(map_);
}

shared_ptr<Buffer> MemoryNamespace::get(shared_ptr<Buffer> key)
{
    auto it = map_.find(key);
    if (it == map_.end())
        throw system_error(ENOENT, system_category());
    return it->second;
}

uint64_t MemoryNamespace::spaceUsed(
    shared_ptr<Buffer> start, shared_ptr<Buffer> end)
{
    return 0;
}

void MemoryIterator::seek(shared_ptr<Buffer> key)
{
    it_ = map_.lower_bound(key);
}

void MemoryIterator::next()
{
    ++it_;
}

bool MemoryIterator::valid(shared_ptr<Buffer> endKey) const
{
    namespaceT::key_compare comp;
    return it_ != map_.end() && comp(it_->first, endKey);
}

shared_ptr<Buffer> MemoryIterator::key() const
{
    return it_->first;
}

shared_ptr<Buffer> MemoryIterator::value() const
{
    return it_->second;
}

void MemoryTransaction::put(
    shared_ptr<Namespace> ns, shared_ptr<Buffer> key, shared_ptr<Buffer> val)
{
    auto mns = dynamic_pointer_cast<MemoryNamespace>(ns);
    ops_.emplace_back(
        [=]() {
            mns->map()[key] = val;
        });
}

void MemoryTransaction::remove(
    shared_ptr<Namespace> ns, shared_ptr<Buffer> key)
{
    auto mns = dynamic_pointer_cast<MemoryNamespace>(ns);
    ops_.emplace_back(
        [=]() {
            mns->map().erase(key);
        });
}

void MemoryTransaction::commit()
{
    for (auto& op: ops_)
        op();
    ops_.clear();
}
