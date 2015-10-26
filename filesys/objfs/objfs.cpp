#include <cassert>
#include <random>
#include <system_error>

#include <rpc++/xdr.h>
#include <fs++/urlparser.h>
#include <glog/logging.h>

#include "objfs.h"
#include "rocksdbi.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace std;

static std::random_device rnd;

ObjFilesystem::ObjFilesystem(const std::string& filename)
{
    db_ = make_unique<RocksDatabase>(filename);

    auto namespaces = db_->open({"default", "directories", "data"});
    defaultNS_ = namespaces[0];
    directoriesNS_ = namespaces[1];
    dataNS_ = namespaces[2];

    try {
        auto buf = db_->get(defaultNS(), KeyType(FileId(0)));
        oncrpc::XdrMemory xm(buf->data(), buf->size());
        xdr(meta_, static_cast<oncrpc::XdrSource*>(&xm));
        if (meta_.vers != 1) {
            LOG(ERROR) << "unexpected filesystem metadata version: "
                << meta_.vers << ", expected: " << 1;
            throw system_error(EACCES, system_category());
        }
    }
    catch (oncrpc::XdrError&) {
        LOG(ERROR) << "error decoding filesystem metadata";
        throw system_error(EACCES, system_category());
    }
    catch (system_error&) {
        meta_.vers = 1;
        for (auto& v: meta_.fsid.data)
            v = rnd();
        meta_.blockSize = 4096;
        meta_.nextId = 2;
        auto trans = db_->beginTransaction();
        writeMeta(trans.get());
        db_->commit(move(trans));
    }
}

ObjFilesystem::~ObjFilesystem()
{
}

std::shared_ptr<File>
ObjFilesystem::root()
{
    using namespace std::chrono;
    if (!root_) {
        try {
            root_ = make_shared<ObjFile>(shared_from_this(), FileId(1));
        }
        catch (system_error& e) {
            ObjFileMetaImpl meta;
            auto time = duration_cast<nanoseconds>(
                system_clock::now().time_since_epoch());
            meta.fileid = FileId(1);
            meta.attr.type = PT_DIR;
            meta.attr.mode = 0755;
            meta.attr.nlink = 2;
            meta.attr.size = 2;
            meta.attr.atime = time.count();
            meta.attr.mtime = time.count();
            meta.attr.ctime = time.count();
            root_ = make_shared<ObjFile>(shared_from_this(), move(meta));
            add(root_);

            // Write the root directory metadata and directory entries for
            // "." and ".."
            auto trans = db_->beginTransaction();
            root_->writeMeta(trans.get());
            root_->writeDirectoryEntry(trans.get(), ".", FileId(1));
            root_->writeDirectoryEntry(trans.get(), "..", FileId(1));
            db_->commit(move(trans));
        }
    }
    return root_;
}

shared_ptr<ObjFile>
ObjFilesystem::find(FileId fileid)
{
    auto i = cache_.find(fileid);
    if (i != cache_.end()) {
        VLOG(2) << "cache hit for fileid: " << fileid;
        auto p = i->second;
        lru_.splice(lru_.begin(), lru_, p);
        return *p;
    }
    else {
        auto file = make_shared<ObjFile>(shared_from_this(), fileid);
        add(file);
        return file;
    }
}

void
ObjFilesystem::add(std::shared_ptr<ObjFile> file)
{
    // Expire old entries if the cache is full
    if (cache_.size() == maxCache_) {
        auto oldest = lru_.back();
        VLOG(2) << "expiring fileid: " << oldest->fileid();
        cache_.erase(oldest->fileid());
        lru_.pop_back();
    }
    VLOG(2) << "adding fileid: " << file->fileid();
    auto p = lru_.insert(lru_.begin(), file);
    cache_[file->fileid()] = p;
}

void
ObjFilesystem::writeMeta(Transaction* trans)
{
    oncrpc::XdrMemory xm(512);
    xdr(meta_, static_cast<oncrpc::XdrSink*>(&xm));
    trans->put(
        defaultNS(),
        KeyType(FileId(0)),
        oncrpc::Buffer(xm.writePos(), xm.buf()));
}

pair<shared_ptr<Filesystem>, string>
ObjFilesystemFactory::mount(FilesystemManager* fsman, const string& url)
{
    UrlParser p(url);
    return make_pair(fsman->mount<ObjFilesystem>(p.path, p.path), ".");
};

void filesys::objfs::init(FilesystemManager* fsman)
{
    fsman->add(make_shared<ObjFilesystemFactory>());
}
