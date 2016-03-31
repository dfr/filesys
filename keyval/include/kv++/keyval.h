#pragma once

#include <memory>
#include <string>
#include <vector>

#include <rpc++/xdr.h>      // for Buffer

namespace keyval {

using oncrpc::Buffer;

class Iterator;
class Namespace;
class Transaction;

/// An interface to a key-value database
class Database
{
public:
    virtual ~Database() {}
    virtual std::shared_ptr<Namespace> getNamespace(
        const std::string& name) = 0;
    virtual std::unique_ptr<Transaction> beginTransaction() = 0;
    virtual void commit(std::unique_ptr<Transaction>&& transaction) = 0;
    virtual void flush() = 0;
};

class Namespace
{
public:
    virtual ~Namespace() {}
    virtual std::unique_ptr<Iterator> iterator() = 0;
    virtual std::shared_ptr<Buffer> get(std::shared_ptr<Buffer> key) = 0;
    virtual std::uint64_t spaceUsed(
        std::shared_ptr<Buffer> start, std::shared_ptr<Buffer> end) = 0;
};

class Iterator
{
public:
    virtual ~Iterator() {}
    virtual void seek(std::shared_ptr<Buffer> key) = 0;
    virtual void next() = 0;
    virtual bool valid(std::shared_ptr<Buffer> endKey) const = 0;
    virtual std::shared_ptr<Buffer> key() const = 0;
    virtual std::shared_ptr<Buffer> value() const = 0;
};

class Transaction
{
public:
    virtual ~Transaction() {}
    virtual void put(
        std::shared_ptr<Namespace> ns,
        std::shared_ptr<Buffer> key,
        std::shared_ptr<Buffer> val) = 0;
    virtual void remove(
        std::shared_ptr<Namespace> ns,
        std::shared_ptr<Buffer> key) = 0;
};

std::unique_ptr<Database> make_memdb();
std::unique_ptr<Database> make_rocksdb(const std::string& filename);

}
