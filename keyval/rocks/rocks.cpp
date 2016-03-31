#include <system_error>

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

unique_ptr<keyval::Iterator> RocksNamespace::iterator()
{
    return make_unique<RocksIterator>(
        db_->NewIterator(ReadOptions(), handle_.get()));
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

void RocksIterator::next()
{
    it_->Next();
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

