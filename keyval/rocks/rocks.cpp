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
    return make_unique<RocksIterator>(db_, handle_.get(), nullptr, nullptr);
}

unique_ptr<keyval::Iterator> RocksNamespace::iterator(
    shared_ptr<Buffer> startKey, shared_ptr<Buffer> endKey)
{
    return make_unique<RocksIterator>(db_, handle_.get(), startKey, endKey);
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

RocksIterator::RocksIterator(
    rocksdb::DB* db,
    rocksdb::ColumnFamilyHandle* ns,
    std::shared_ptr<Buffer> startKey,
    std::shared_ptr<Buffer> endKey)
{
    ReadOptions opts;
    if (endKey) {
        endKey_ = Slice(
            reinterpret_cast<const char*>(endKey->data()), endKey->size());
        opts.iterate_upper_bound = &endKey_;
    }
    it_.reset(move(db->NewIterator(opts, ns)));
    if (startKey) {
        startKey_ = Slice(
            reinterpret_cast<const char*>(startKey->data()), startKey->size());
        it_->Seek(startKey_);
    }
    else {
        it_->SeekToFirst();
    }
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

