#pragma once

#include "filesys/objfs/dbi.h"
#include <rocksdb/db.h>

namespace filesys {
namespace objfs {

class RocksDatabase: public Database
{
public:
    RocksDatabase(const std::string& filename);
    ~RocksDatabase() override;

    // Database overrides
    std::vector<Namespace*> open(
            std::vector<std::string> namespaces) override;
    std::unique_ptr<Transaction> beginTransaction() override;
    void commit(std::unique_ptr<Transaction>&& transaction) override;
    std::unique_ptr<Buffer> get(Namespace* ns, const Buffer& key) override;
    std::unique_ptr<Iterator> iterator(Namespace* ns) override;
    std::uint64_t spaceUsed(
        Namespace* ns, const Buffer& start, const Buffer& end) override;
    void flush() override;

private:
    std::string filename_;
    std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>> namespaces_;
    std::unique_ptr<rocksdb::DB> db_;
};

class RocksIterator: public Iterator
{
public:
    RocksIterator(rocksdb::Iterator* it) : it_(it) {}
    void seek(const Buffer& key) override;
    void next() override;
    bool valid(const Buffer& endKey) const override;
    std::unique_ptr<Buffer> key() const override;
    std::unique_ptr<Buffer> value() const override;

private:
    std::unique_ptr<rocksdb::Iterator> it_;
    Buffer key_;
    Buffer value_;
};

class RocksTransaction: public Transaction
{
public:
    void put(
        Namespace* ns, const Buffer& key, const Buffer& val) override;
    void remove(Namespace* ns, const Buffer& key) override;

    auto batch() { return &batch_; }

private:
    rocksdb::WriteBatch batch_;
};

}
}
