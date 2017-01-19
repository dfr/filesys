/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <keyval/keyval.h>
#include <rocksdb/db.h>
#include <rocksdb/cache.h>
#include <rocksdb/table.h>

namespace keyval {
namespace rocks {

class RocksNamespace;

class RocksDatabase: public Database
{
public:
    RocksDatabase(const std::string& filename);
    ~RocksDatabase() override;

    // Database overrides
    std::shared_ptr<Namespace> getNamespace(const std::string& name) override;
    std::unique_ptr<Transaction> beginTransaction() override;
    void commit(std::unique_ptr<Transaction>&& transaction) override;
    void flush() override;
    bool isReplicated() override { return false; }
    bool isMaster() override { return true; }
    bool get(
        std::shared_ptr<oncrpc::RestRequest> req,
        std::unique_ptr<oncrpc::RestEncoder>&& res) override;
    void onMasterChange(std::function<void(bool)> cb) override {}
    void setAppData(const std::vector<uint8_t>& data) override {}
    std::vector<ReplicaInfo> getReplicas() override { return {}; }

private:
    std::string filename_;
    std::unordered_map<std::string, std::shared_ptr<RocksNamespace>>
        namespaces_;
    std::unique_ptr<rocksdb::DB> db_;
};

class RocksNamespace: public Namespace
{
public:
    RocksNamespace(
        rocksdb::DB* db, rocksdb::ColumnFamilyHandle* handle)
        : db_(db),
          handle_(handle)
    {
    }

    std::unique_ptr<Iterator> iterator() override;
    std::unique_ptr<Iterator> iterator(
        std::shared_ptr<Buffer> startKey, std::shared_ptr<Buffer> endKey) override;
    std::shared_ptr<Buffer> get(std::shared_ptr<Buffer> key) override;
    std::uint64_t spaceUsed(
        std::shared_ptr<Buffer> start, std::shared_ptr<Buffer> end) override;

    rocksdb::ColumnFamilyHandle* handle() const
    {
        return handle_.get();
    }

    void clearHandle()
    {
        handle_.reset();
    }

private:
    rocksdb::DB* db_;
    std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;
};

class RocksIterator: public Iterator
{
public:
    RocksIterator(
        rocksdb::DB* db,
        rocksdb::ColumnFamilyHandle* ns,
        std::shared_ptr<Buffer> startKey,
        std::shared_ptr<Buffer> endKey);
    void seek(std::shared_ptr<Buffer> key) override;
    void seekToFirst() override;
    void seekToLast() override;
    void next() override;
    void prev() override;
    bool valid() const override;
    std::shared_ptr<Buffer> key() const override;
    std::shared_ptr<Buffer> value() const override;

private:
    std::unique_ptr<rocksdb::Iterator> it_;
    rocksdb::Slice startKey_;
    rocksdb::Slice endKey_;
};

class RocksTransaction: public Transaction
{
public:
    void put(
        std::shared_ptr<Namespace> ns,
        std::shared_ptr<Buffer> key, std::shared_ptr<Buffer> val) override;
    void remove(
        std::shared_ptr<Namespace> ns, std::shared_ptr<Buffer> key) override;

    auto batch() { return &batch_; }

private:
    rocksdb::WriteBatch batch_;
};

}
}
