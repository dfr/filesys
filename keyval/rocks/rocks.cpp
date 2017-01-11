/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#include <system_error>

#include <rpc++/rest.h>
#include "rocks.h"

using namespace keyval;
using namespace keyval::rocks;
using namespace rocksdb;
using namespace std;

RocksDatabase::RocksDatabase(const string& filename)
    : filename_(filename)
{
    Status status;
    DB* db;
    DBOptions options;

    options.create_if_missing = true;
    options.keep_log_file_num = 5;

    vector<string> cfnames;
    DB::ListColumnFamilies(options, filename_, &cfnames);
    if (cfnames.size() == 0)
        cfnames.push_back(kDefaultColumnFamilyName);

    vector<ColumnFamilyDescriptor> descriptors;
    vector<ColumnFamilyHandle*> handles;
    for (auto& cfname: cfnames)
        descriptors.emplace_back(cfname, ColumnFamilyOptions());

    status = DB::Open(options, filename_, descriptors, &handles, &db);
    assert(status.ok());

    for (int i = 0; i < int(cfnames.size()); i++) {
        namespaces_[cfnames[i]] =
            make_shared<RocksNamespace>(db, handles[i]);
    }

    db_.reset(db);
}

RocksDatabase::~RocksDatabase()
{
    for (auto& ns: namespaces_)
        ns.second->clearHandle();
    db_.reset();
}

// Database overrides
shared_ptr<Namespace> RocksDatabase::getNamespace(const string& name)
{
    auto it = namespaces_.find(name);
    if (it != namespaces_.end())
        return it->second;

    Status status;
    ColumnFamilyHandle* h;
    status = db_->CreateColumnFamily(ColumnFamilyOptions(), name, &h);
    assert(status.ok());
    auto ns = make_shared<RocksNamespace>(db_.get(), h);
    namespaces_[name] = ns;

    return ns;
}

unique_ptr<Transaction> RocksDatabase::beginTransaction()
{
    return make_unique<RocksTransaction>();
}

void RocksDatabase::commit(unique_ptr<Transaction>&& transaction)
{
    auto p = reinterpret_cast<RocksTransaction*>(transaction.get());
    auto status = db_->Write(WriteOptions(), p->batch());
    assert(status.ok());
    transaction.reset();
}

void RocksDatabase::flush()
{
    db_->SyncWAL();
}

bool RocksDatabase::get(
    std::shared_ptr<oncrpc::RestRequest> req,
    std::unique_ptr<oncrpc::RestEncoder>&& res)
{
    auto obj = res->object();
    uint64_t val;

    auto options = db_->GetOptions();
    BlockBasedTableOptions* bbo = reinterpret_cast<BlockBasedTableOptions*>(
        options.table_factory->GetOptions());
    obj->field("blockCacheMem")->number(long(bbo->block_cache->GetUsage()));

    db_->GetIntProperty("rocksdb.estimate-table-readers-mem", &val);
    obj->field("tableReadersMem")->number(long(val));

    db_->GetIntProperty("rocksdb.cur-size-all-mem-tables", &val);
    obj->field("memtableSize")->number(long(val));

    return true;
}

unique_ptr<keyval::Iterator> RocksNamespace::iterator()
{
    auto iter = db_->NewIterator(ReadOptions(), handle_.get());
    iter->SeekToFirst();
    return make_unique<RocksIterator>(iter);
}

unique_ptr<keyval::Iterator> RocksNamespace::iterator(shared_ptr<Buffer> key)
{
    auto iter = db_->NewIterator(ReadOptions(), handle_.get());
    iter->Seek(Slice(reinterpret_cast<const char*>(key->data()), key->size()));
    return make_unique<RocksIterator>(iter);
}


shared_ptr<Buffer> RocksNamespace::get(shared_ptr<Buffer> key)
{
    string val;
    auto status = db_->Get(
        ReadOptions(), handle_.get(),
        Slice(reinterpret_cast<const char*>(key->data()), key->size()),
        &val);
    if (status.IsNotFound())
        throw system_error(ENOENT, system_category());
    // XXX: avoid copy somehow?
    auto res = make_shared<Buffer>(val.size());
    copy_n(
        reinterpret_cast<const uint8_t*>(val.data()), val.size(), res->data());
    return res;
}

uint64_t RocksNamespace::spaceUsed(
    shared_ptr<Buffer> start, shared_ptr<Buffer> end)
{
    Range range(
        Slice(reinterpret_cast<const char*>(start->data()), start->size()),
        Slice(reinterpret_cast<const char*>(end->data()), end->size()));
    uint64_t sz;
    db_->GetApproximateSizes(handle_.get(), &range, 1, &sz, true);
    return sz;
}

void RocksIterator::seek(shared_ptr<Buffer> key)
{
    it_->Seek(Slice(reinterpret_cast<const char*>(key->data()), key->size()));
}

void RocksIterator::seekToFirst()
{
    it_->SeekToFirst();
}

void RocksIterator::seekToLast()
{
    it_->SeekToLast();
}

void RocksIterator::next()
{
    it_->Next();
}

void RocksIterator::prev()
{
    it_->Prev();
}

bool RocksIterator::valid() const
{
    return it_->Valid();
}

bool RocksIterator::valid(shared_ptr<Buffer> endKey) const
{
    Slice ek(reinterpret_cast<const char*>(endKey->data()), endKey->size());
    return it_->Valid() && it_->key().compare(ek) < 0;
}

shared_ptr<Buffer> RocksIterator::key() const
{
    auto k = it_->key();
    auto res = make_shared<Buffer>(k.size());
    copy_n(reinterpret_cast<const uint8_t*>(k.data()), k.size(), res->data());
    return res;
}

shared_ptr<Buffer> RocksIterator::value() const
{
    auto v = it_->value();
    auto res = make_shared<Buffer>(v.size());
    copy_n(reinterpret_cast<const uint8_t*>(v.data()), v.size(), res->data());
    return res;
}

void RocksTransaction::put(
    shared_ptr<Namespace> ns, shared_ptr<Buffer> key, shared_ptr<Buffer> val)
{
    auto ons = dynamic_pointer_cast<RocksNamespace>(ns);
    batch_.Put(
        ons->handle(),
        Slice(reinterpret_cast<const char*>(key->data()), key->size()),
        Slice(reinterpret_cast<const char*>(val->data()), val->size()));
}

void RocksTransaction::remove(
    shared_ptr<Namespace> ns, shared_ptr<Buffer> key)
{
    auto ons = dynamic_pointer_cast<RocksNamespace>(ns);
    batch_.Delete(
        ons->handle(),
        Slice(reinterpret_cast<const char*>(key->data()), key->size()));
}

