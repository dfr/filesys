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
        setFsid();
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
            meta.attr.nlink = 0;
            meta.attr.size = 0;
            meta.attr.atime = time.count();
            meta.attr.mtime = time.count();
            meta.attr.ctime = time.count();
            setFsid();
            root_ = make_shared<ObjFile>(shared_from_this(), move(meta));
            add(root_);

            // Write the root directory metadata and directory entries for
            // "." and ".."
            auto trans = db_->beginTransaction();
            root_->link(trans.get(), ".", root_.get(), false);
            root_->link(trans.get(), "..", root_.get(), false);
            root_->writeMeta(trans.get());
            db_->commit(move(trans));
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
    assert(fh.fsid == fsid_);
    try {
        return find(*reinterpret_cast<const FileId*>(fh.handle.data()));
    }
    catch (system_error&) {
        throw system_error(ESTALE, system_category());
    }
}

shared_ptr<ObjFile>
ObjFilesystem::find(FileId fileid)
{
    return cache_.find(
        fileid,
        [](auto) {},
        [this](auto id) {
            return make_shared<ObjFile>(shared_from_this(), FileId(id));
        });
}

void
ObjFilesystem::add(std::shared_ptr<ObjFile> file)
{
    cache_.add(file->fileid(), file);
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

void
ObjFilesystem::setFsid()
{
    fsid_.resize(sizeof(meta_.fsid));
    *reinterpret_cast<UUID*>(fsid_.data()) = meta_.fsid;
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
