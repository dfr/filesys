#include <system_error>

#include "filesys/objfs/rocksdbi.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace rocksdb;
using namespace std;

RocksDatabase::RocksDatabase(const string& filename)
    : filename_(filename)
{
}

RocksDatabase::~RocksDatabase()
{
    namespaces_.clear();
    db_.reset();
}

// Database overrides
vector<Namespace*> RocksDatabase::open(vector<string> namespaces)
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

    for (auto& ns: namespaces) {
        if (find(cfnames.begin(), cfnames.end(), ns) == cfnames.end()) {
            ColumnFamilyHandle* h;
            status = db->CreateColumnFamily(
                ColumnFamilyOptions(), ns, &h);
            assert(status.ok());
            handles.push_back(h);
        }
    }

    vector<Namespace*> res;
    for (auto h: handles) {
        namespaces_.emplace_back(h);
        res.push_back(reinterpret_cast<Namespace*>(h));
    }
    db_.reset(db);

    return res;
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

std::unique_ptr<Buffer> RocksDatabase::get(Namespace* ns, const Buffer& key)
{
    string val;
    auto status = db_->Get(
        ReadOptions(), reinterpret_cast<ColumnFamilyHandle*>(ns),
        Slice(reinterpret_cast<const char*>(key.data()), key.size()),
        &val);
    if (status.IsNotFound())
        throw system_error(ENOENT, system_category());
    // XXX: avoid copy somehow?
    auto res = make_unique<Buffer>(val.size());
    copy_n(
        reinterpret_cast<const uint8_t*>(val.data()), val.size(), res->data());
    return res;
}

std::unique_ptr<filesys::objfs::Iterator> RocksDatabase::iterator(Namespace* ns)
{
    return make_unique<RocksIterator>(db_->NewIterator(
        ReadOptions(), reinterpret_cast<ColumnFamilyHandle*>(ns)));
}

std::uint64_t RocksDatabase::spaceUsed(
    Namespace* ns, const Buffer& start, const Buffer& end)
{
    Range range(
        Slice(reinterpret_cast<const char*>(start.data()), start.size()),
        Slice(reinterpret_cast<const char*>(end.data()), end.size()));
    std::uint64_t sz;
    db_->GetApproximateSizes(
        reinterpret_cast<ColumnFamilyHandle*>(ns), &range, 1, &sz, true);
    return sz;
}

void RocksDatabase::flush()
{
    db_->SyncWAL();
}

void RocksIterator::seek(const Buffer& key)
{
    it_->Seek(Slice(reinterpret_cast<const char*>(key.data()), key.size()));
}

void RocksIterator::next()
{
    it_->Next();
}

bool RocksIterator::valid(const Buffer& endKey) const
{
    Slice ek(reinterpret_cast<const char*>(endKey.data()), endKey.size());
    return it_->Valid() && it_->key().compare(ek) < 0;
}

unique_ptr<Buffer> RocksIterator::key() const
{
    auto k = it_->key();
    return make_unique<Buffer>(
        k.size(), reinterpret_cast<uint8_t*>(const_cast<char*>(k.data())));
}

unique_ptr<Buffer> RocksIterator::value() const
{
    auto v = it_->value();
    return make_unique<Buffer>(
        v.size(), reinterpret_cast<uint8_t*>(const_cast<char*>(v.data())));
}

void RocksTransaction::put(Namespace* ns, const Buffer& key, const Buffer& val)
{
    batch_.Put(
        reinterpret_cast<ColumnFamilyHandle*>(ns),
        Slice(reinterpret_cast<const char*>(key.data()), key.size()),
        Slice(reinterpret_cast<const char*>(val.data()), val.size()));
}

void RocksTransaction::remove(Namespace* ns, const Buffer& key)
{
    batch_.Delete(
        reinterpret_cast<ColumnFamilyHandle*>(ns),
        Slice(reinterpret_cast<const char*>(key.data()), key.size()));
}
