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

    /// Create a new iterator which is set to iterate from the start
    /// of the namespace
    virtual std::unique_ptr<Iterator> iterator() = 0;

    /// Create a new iterator which is set to iterate from the given key
    virtual std::unique_ptr<Iterator> iterator(std::shared_ptr<Buffer> key) = 0;

    virtual std::shared_ptr<Buffer> get(std::shared_ptr<Buffer> key) = 0;
    virtual std::uint64_t spaceUsed(
        std::shared_ptr<Buffer> start, std::shared_ptr<Buffer> end) = 0;
};

class Iterator
{
public:
    virtual ~Iterator() {}

    /// Seek to the first entry greater than or equal to the given key
    virtual void seek(std::shared_ptr<Buffer> key) = 0;

    /// Seek to the first entry in the namespace
    virtual void seekToFirst() = 0;

    /// Seek to the last entry in the namespace
    virtual void seekToLast() = 0;

    /// Advance to the next entry in the namespace
    virtual void next() = 0;

    /// Advance to the previous entry in the namespace
    virtual void prev() = 0;

    /// Return true if the iterator references a valid entry
    virtual bool valid() const = 0;

    /// Return true if the iterator references a valid entry and the
    /// entry's key is less than the given endKey
    virtual bool valid(std::shared_ptr<Buffer> endKey) const = 0;

    /// Return the current entry's key
    virtual std::shared_ptr<Buffer> key() const = 0;

    /// Return the current entry's value
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
