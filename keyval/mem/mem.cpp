/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <cstring>
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
    unique_lock<mutex> lock(mutex_);

    auto it = namespaces_.find(name);
    if (it != namespaces_.end())
        return it->second;

    auto ns = make_shared<MemoryNamespace>(mutex_);
    namespaces_[name] = ns;

    return ns;
}

unique_ptr<Transaction> MemoryDatabase::beginTransaction()
{
    return make_unique<MemoryTransaction>();
}

void MemoryDatabase::commit(unique_ptr<Transaction>&& transaction)
{
    unique_lock<mutex> lock(mutex_);
    auto t = reinterpret_cast<MemoryTransaction*>(transaction.get());
    t->commit();
    transaction.reset();
}

unique_ptr<Iterator> MemoryNamespace::iterator()
{
    return make_unique<MemoryIterator>(*this);
}

unique_ptr<Iterator> MemoryNamespace::iterator(shared_ptr<Buffer> key)
{
    return make_unique<MemoryIterator>(*this, key);
}

shared_ptr<Buffer> MemoryNamespace::get(shared_ptr<Buffer> key)
{
    auto lk = lock();
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

void MemoryNamespace::put(shared_ptr<Buffer> key, shared_ptr<Buffer> value)
{
    gen_++;
    map_[key] = value;
}

void MemoryNamespace::remove(shared_ptr<Buffer> key)
{
    gen_++;
    map_.erase(key);
}

void MemoryIterator::seek(shared_ptr<Buffer> key)
{
    auto lk = ns_.lock();
    gen_ = ns_.gen();
    it_ = ns_.map().lower_bound(key);
    read();
}

void MemoryIterator::seekToFirst()
{
    auto lk = ns_.lock();
    gen_ = ns_.gen();
    it_ = ns_.map().begin();
    read();
}

void MemoryIterator::seekToLast()
{
    auto lk = ns_.lock();
    auto key = ns_.map().rbegin()->first;
    lk.unlock();
    seek(key);
}


void MemoryIterator::next()
{
    auto lk = ns_.lock();
    if (gen_ != ns_.gen()) {
        gen_ = ns_.gen();
        it_ = ns_.map().lower_bound(key_);
    }
    ++it_;
    read();
}

void MemoryIterator::prev()
{
    auto lk = ns_.lock();
    if (gen_ != ns_.gen()) {
        gen_ = ns_.gen();
        it_ = ns_.map().lower_bound(key_);
    }
    --it_;
    read();
}

bool MemoryIterator::valid() const
{
    return valid_;
}

bool MemoryIterator::valid(shared_ptr<Buffer> endKey) const
{
    namespaceT::key_compare comp;
    return valid_ && comp(key_, endKey);
}

shared_ptr<Buffer> MemoryIterator::key() const
{
    return key_;
}

shared_ptr<Buffer> MemoryIterator::value() const
{
    return value_;
}

void MemoryTransaction::put(
    shared_ptr<Namespace> ns, shared_ptr<Buffer> key, shared_ptr<Buffer> val)
{
    auto mns = dynamic_pointer_cast<MemoryNamespace>(ns);
    ops_.emplace_back(
        [=]() {
            mns->put(key, val);
        });
}

void MemoryTransaction::remove(
    shared_ptr<Namespace> ns, shared_ptr<Buffer> key)
{
    auto mns = dynamic_pointer_cast<MemoryNamespace>(ns);
    ops_.emplace_back(
        [=]() {
            mns->remove(key);
        });
}

void MemoryTransaction::commit()
{
    for (auto& op: ops_)
        op();
    ops_.clear();
}
