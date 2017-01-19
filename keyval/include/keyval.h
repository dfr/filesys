/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include <rpc++/xdr.h>      // for Buffer

namespace util {
class Clock;
}

namespace oncrpc {
class RestEncoder;
class RestRequest;
class SocketManager;
}

namespace keyval {

using oncrpc::Buffer;

class Iterator;
class Namespace;
class Transaction;

/// Current state of a database replica
struct ReplicaInfo
{
    enum State {
        DEAD,
        HEALTHY,
        RECOVERING,
        UNKNOWN
    };

    /// Current replica state
    State state;

    /// Application data for this replica
    std::vector<uint8_t> appdata;
};

/// An interface to a key-value database
class Database
{
public:
    virtual ~Database() {}

    /// Get a pointer to the given namespace object
    virtual std::shared_ptr<Namespace> getNamespace(
        const std::string& name) = 0;

    /// Start a new transaction
    virtual std::unique_ptr<Transaction> beginTransaction() = 0;

    /// Commit the transaction to the database
    virtual void commit(std::unique_ptr<Transaction>&& transaction) = 0;

    /// Flush any commit transactions to stable storage
    virtual void flush() = 0;

    /// Return true if this database is replicated
    virtual bool isReplicated() = 0;

    /// For replicated databases, return true if this instance is the
    /// 'master' replica
    virtual bool isMaster() = 0;

    /// Databases can implement this to allow exporting metrics
    virtual bool get(
        std::shared_ptr<oncrpc::RestRequest> req,
        std::unique_ptr<oncrpc::RestEncoder>&& res) = 0;

    /// Register a callback function which is called if the database
    /// master state changes. The callback function is called with a
    /// bool argument which is true if the database is the new master
    /// or false if it is a replica.
    virtual void onMasterChange(std::function<void(bool)> cb) = 0;

    // Extra application-layer data associated with this database,
    // typically used to coordinate information between instances in a
    // replicated database
    virtual void setAppData(const std::vector<uint8_t>& data) = 0;

    // Return the state for each replica with the state for the
    // current master listed first
    virtual std::vector<ReplicaInfo> getReplicas() = 0;
};

/// Key/value pairs are grouped by namespace
class Namespace
{
public:
    virtual ~Namespace() {}

    /// Create a new iterator which is set to iterate over the entire
    /// namespace
    virtual std::unique_ptr<Iterator> iterator() = 0;

    /// Create a new iterator which is set to iterate from the given
    /// start key up to but not including the end key
    virtual std::unique_ptr<Iterator> iterator(
        std::shared_ptr<Buffer> startKey, std::shared_ptr<Buffer> endKey) = 0;

    /// Get the value for a given key in this namespace
    virtual std::shared_ptr<Buffer> get(std::shared_ptr<Buffer> key) = 0;

    /// Return an approximate indication of the space used by this namespace
    virtual std::uint64_t spaceUsed(
        std::shared_ptr<Buffer> start, std::shared_ptr<Buffer> end) = 0;
};

/// Iterator objects are used to iterate through the key/value pairs
/// in a namespace
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

    /// Return the current entry's key
    virtual std::shared_ptr<Buffer> key() const = 0;

    /// Return the current entry's value
    virtual std::shared_ptr<Buffer> value() const = 0;
};

/// A set of write operations to a database which must all be executed
/// together atomically
class Transaction
{
public:
    virtual ~Transaction() {}

    /// Write a new value for a key in the given namespace
    virtual void put(
        std::shared_ptr<Namespace> ns,
        std::shared_ptr<Buffer> key,
        std::shared_ptr<Buffer> val) = 0;

    /// Remove a key/value pair from the given namespace
    virtual void remove(
        std::shared_ptr<Namespace> ns,
        std::shared_ptr<Buffer> key) = 0;
};

/// Create an in-memory database - typically used for unit tests
std::shared_ptr<Database> make_memdb();

/// Create a database backed by RocksDB
std::shared_ptr<Database> make_rocksdb(const std::string& filename);

/// Create a Paxos replicated database
///
/// - filename is the path to the underlying RocksDB
/// - addr is a URL to bind to for replica updates
/// - replicas is a set of target URLs for replica updates
/// - sockman is used to register sockets and timeouts
///
std::shared_ptr<Database> make_paxosdb(
    const std::string& filename,
    const std::vector<std::string>& replicas);

}
