/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <cassert>
#include <iomanip>
#include <random>
#include <system_error>

#include <rpc++/urlparser.h>
#include <rpc++/xdr.h>
#include <glog/logging.h>

#include "objfs.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace keyval;
using namespace std;

static std::random_device rnd;

ObjFilesystem::ObjFilesystem(
    shared_ptr<Database> db, shared_ptr<util::Clock> clock,
    uint64_t blockSize)
    : clock_(clock),
      db_(db),
      blockSize_(blockSize)
{
    defaultNS_ = db_->getNamespace("default");
    directoriesNS_ = db_->getNamespace("directories");
    dataNS_ = db_->getNamespace("data");

again:
    try {
        auto buf = defaultNS_->get(KeyType(FileId(0)));
        oncrpc::XdrMemory xm(buf->data(), buf->size());
        xdr(meta_, static_cast<oncrpc::XdrSource*>(&xm));
        if (meta_.vers != 1) {
            LOG(ERROR) << "unexpected filesystem metadata version: "
                << meta_.vers << ", expected: " << 1;
            throw system_error(EACCES, system_category());
        }
        nextId_ = meta_.nextId;
    }
    catch (oncrpc::XdrError&) {
        LOG(ERROR) << "error decoding filesystem metadata";
        throw system_error(EACCES, system_category());
    }
    catch (system_error&) {
        if (!db_->isMaster()) {
            // If we are not the master replica, just try again - the
            // master will write the metadata entry
            std::this_thread::sleep_for(std::chrono::seconds(1));
            goto again;
        }
        meta_.vers = 1;
        for (auto& v: meta_.fsid.data)
            v = rnd();
        meta_.nextId = 2;
        nextId_ = meta_.nextId;
        auto trans = db_->beginTransaction();
        writeMeta(trans.get());
        db_->commit(move(trans));
    }
    setFsid();

    // Register a callback for database master changes
    if (db) {
        using namespace std::placeholders;
        db->onMasterChange(
            std::bind(&ObjFilesystem::databaseMasterChanged, this, _1));
    }
}

ObjFilesystem::ObjFilesystem(shared_ptr<Database> db, uint64_t blockSize)
    : ObjFilesystem(db, make_shared<util::SystemClock>(), blockSize)
{
}

ObjFilesystem::~ObjFilesystem()
{
}

std::shared_ptr<File>
ObjFilesystem::root()
{
    using namespace std::chrono;
    if (!root_) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (root_) {
            // Someone else may have created the root object while we were
            // waiting for the lock
            return root_;
        }
        try {
            root_ = makeNewFile(FileId(1));
        }
        catch (system_error& e) {
            // It would be nice to move this to the constructor but
            // shared_from_this() doesn't work there.
            ObjFileMetaImpl meta;
            auto time = duration_cast<nanoseconds>(
                clock_->now().time_since_epoch());
            meta.fileid = FileId(1);
            meta.blockSize = blockSize_;
            meta.attr.type = PT_DIR;
            meta.attr.mode = 0755;
            meta.attr.nlink = 0;
            meta.attr.size = 0;
            meta.attr.atime = time.count();
            meta.attr.mtime = time.count();
            meta.attr.ctime = time.count();
            meta.attr.birthtime = time.count();
            root_ = makeNewFile(move(meta));
            add(root_);

            if (db_->isMaster()) {
                // Write the root directory metadata and directory entries for
                // "." and ".."
                auto trans = db_->beginTransaction();
                root_->link(trans.get(), ".", root_.get(), false);
                root_->link(trans.get(), "..", root_.get(), false);
                root_->writeMeta(trans.get());
                db_->commit(move(trans));
            }
        }
    }
    return root_;
}

const FilesystemId&
ObjFilesystem::fsid() const
{
    return fsid_;
}

shared_ptr<File>
ObjFilesystem::find(const FileHandle& fh)
{
    try {
        oncrpc::XdrMemory xm(
            fh.handle.data() + fsid_.size(), sizeof(std::uint64_t));
        std::uint64_t val;
        xdr(val, static_cast<oncrpc::XdrSource*>(&xm));
        return find(FileId(val));
    }
    catch (system_error&) {
        throw system_error(ESTALE, system_category());
    }
}

shared_ptr<Database>
ObjFilesystem::database() const
{
    return db_;
}

shared_ptr<ObjFile>
ObjFilesystem::find(FileId fileid)
{
    return cache_.find(
        fileid,
        [](auto) {},
        [this](uint64_t id) {
            return makeNewFile(FileId(id));
        });
}

shared_ptr<ObjFile>
ObjFilesystem::makeNewFile(FileId fileid)
{
    return make_shared<ObjFile>(shared_from_this(), fileid);
}

shared_ptr<ObjFile>
ObjFilesystem::makeNewFile(ObjFileMetaImpl&& meta)
{
    return make_shared<ObjFile>(shared_from_this(), move(meta));
}

shared_ptr<OpenFile>
ObjFilesystem::makeNewOpenFile(
    const Credential& cred, shared_ptr<ObjFile> file, int flags)
{
    return make_shared<ObjOpenFile>(cred, file, flags);
}

void
ObjFilesystem::remove(FileId fileid)
{
    cache_.remove(fileid);
}

void
ObjFilesystem::add(std::shared_ptr<ObjFile> file)
{
    cache_.add(file->fileid(), file);
}

void
ObjFilesystem::writeMeta(Transaction* trans)
{
    std::unique_lock<std::mutex> lock(mutex_);
    oncrpc::XdrMemory xm(512);
    assert(nextId_ >= meta_.nextId);
    meta_.nextId = nextId_;
    xdr(meta_, static_cast<oncrpc::XdrSink*>(&xm));
    trans->put(
        defaultNS(),
        KeyType(FileId(0)),
        make_shared<oncrpc::Buffer>(xm.writePos(), xm.buf()));
}

static std::ostream& operator<<(std::ostream& os, const UUID& id)
{
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&id);
    auto savefill = os.fill();
    auto saveflags = os.flags();
    os << std::hex << std::setfill('0');
    os << std::setw(2) << int(p[0]);
    os << std::setw(2) << int(p[1]);
    os << std::setw(2) << int(p[2]);
    os << std::setw(2) << int(p[3]);
    os << "-";
    os << std::setw(2) << int(p[4]);
    os << std::setw(2) << int(p[5]);
    os << "-";
    os << std::setw(2) << int(p[6]);
    os << std::setw(2) << int(p[7]);
    os << "-";
    os << std::setw(2) << int(p[8]);
    os << std::setw(2) << int(p[9]);
    os << "-";
    os << std::setw(2) << int(p[10]);
    os << std::setw(2) << int(p[11]);
    os << std::setw(2) << int(p[12]);
    os << std::setw(2) << int(p[13]);
    os << std::setw(2) << int(p[14]);
    os << std::setw(2) << int(p[15]);
    os.fill(savefill);
    os.flags(saveflags);
    return os;
}

void
ObjFilesystem::setFsid()
{
    LOG(INFO) << "fsid: " << meta_.fsid;
    fsid_.resize(sizeof(meta_.fsid));
    *reinterpret_cast<UUID*>(fsid_.data()) = meta_.fsid;
}

void
ObjFilesystem::databaseMasterChanged(bool isMaster)
{
    LOG(INFO) << "database is " << (isMaster ? "master" : "replica")
              << ": flushing cache";

    // Drop any unreferenced entries from the cache and refresh
    // metadata for the rest
    cache_.clear();
    for (auto& entry: cache_) {
        entry.second->readMeta();
    }

    if (isMaster) {
        // Re-read filesystem metadata to get the correct value of nextId_
        try {
            auto buf = defaultNS_->get(KeyType(FileId(0)));
            oncrpc::XdrMemory xm(buf->data(), buf->size());
            xdr(meta_, static_cast<oncrpc::XdrSource*>(&xm));
            if (meta_.vers != 1) {
                LOG(ERROR) << "unexpected filesystem metadata version: "
                           << meta_.vers << ", expected: " << 1;
                throw system_error(EACCES, system_category());
            }
            nextId_ = meta_.nextId;
            LOG(INFO) << "next fileid: " << nextId_;
        }
        catch (oncrpc::XdrError&) {
            LOG(ERROR) << "error decoding filesystem metadata";
            throw system_error(EACCES, system_category());
        }
        catch (system_error& e) {
            LOG(ERROR) << "no filesystem metadata: " << e.what();
        }
    }
}

shared_ptr<Filesystem> ObjFilesystemFactory::mount(const std::string& url)
{
    oncrpc::UrlParser p(url);
    vector<string> replicas;

    auto range = p.query.equal_range("replica");
    for (auto it = range.first; it != range.second; ++it)
        replicas.push_back(it->second);

    shared_ptr<Database> db;
    if (replicas.size() > 0) {
        db = make_paxosdb(p.path, replicas);
    }
    else {
        db = make_rocksdb(p.path);
    }

    return make_shared<ObjFilesystem>(db);
}

void filesys::objfs::init(FilesystemManager* fsman)
{
    oncrpc::UrlParser::addPathbasedScheme("objfs");
    fsman->add(make_shared<ObjFilesystemFactory>());
}
