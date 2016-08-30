/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <keyval/keyval.h>
#include <rocksdb/db.h>

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
    void onMasterChange(std::function<void(bool)> cb) override {}

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
    std::unique_ptr<Iterator> iterator(std::shared_ptr<Buffer> key) override;
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
    RocksIterator(rocksdb::Iterator* it) : it_(it) {}
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
    std::unique_ptr<rocksdb::Iterator> it_;
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
