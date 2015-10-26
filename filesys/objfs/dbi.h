#pragma once

#include <memory>
#include <string>
#include <vector>

#include <rpc++/xdr.h>      // for Buffer

namespace filesys {
namespace objfs {

using oncrpc::Buffer;

class Iterator;
class Namespace;
class Transaction;

/// An interface to a key-value database
class Database
{
public:
    virtual ~Database() {}
    virtual std::vector<Namespace*> open(
        std::vector<std::string> namespaces) = 0;
    virtual std::unique_ptr<Transaction> beginTransaction() = 0;
    virtual void commit(std::unique_ptr<Transaction>&& transaction) = 0;
    virtual std::unique_ptr<Buffer> get(Namespace* ns, const Buffer& key) = 0;
    virtual std::unique_ptr<Iterator> iterator(Namespace* ns) = 0;
    virtual std::uint64_t spaceUsed(
        Namespace* ns, const Buffer& start, const Buffer& end) = 0;
    virtual void flush() = 0;
};

class Iterator
{
public:
    virtual ~Iterator() {}
    virtual void seek(const Buffer& key) = 0;
    virtual void next() = 0;
    virtual bool valid(const Buffer& endKey) const = 0;
    virtual std::unique_ptr<Buffer> key() const = 0;
    virtual std::unique_ptr<Buffer> value() const = 0;
};

class Transaction
{
public:
    virtual ~Transaction() {}
    virtual void put(
        Namespace* ns, const Buffer& key, const Buffer& val) = 0;
    virtual void remove(Namespace* ns, const Buffer& key) = 0;
};

}
}
