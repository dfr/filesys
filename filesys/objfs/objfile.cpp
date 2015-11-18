#include <cassert>
#include <system_error>
#include <fcntl.h>

#include <glog/logging.h>

#include "objfs.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace std;
using namespace std::chrono;

static uint64_t getTime()
{
    return duration_cast<nanoseconds>(
        system_clock::now().time_since_epoch()).count();
}

ObjFile::ObjFile(shared_ptr<ObjFilesystem> fs, FileId fileid)
    : fs_(fs)
{
    KeyType key(fileid);
    auto buf = fs->db()->get(fs->defaultNS(), key);
    try {
        oncrpc::XdrMemory xm(buf->data(), buf->size());
        xdr(meta_, static_cast<oncrpc::XdrSource*>(&xm));
        if (meta_.vers != 1) {
            LOG(ERROR) << "unexpected file metadata version: "
                << meta_.vers << ", expected: " << 1;
            throw system_error(EACCES, system_category());
        }
    }
    catch (oncrpc::XdrError&) {
        LOG(ERROR) << "error decoding file metadata";
        throw system_error(EACCES, system_category());
    }
}

ObjFile::ObjFile(shared_ptr<ObjFilesystem> fs, ObjFileMetaImpl&& meta)
    : fs_(fs),
      meta_(move(meta))
{
}

ObjFile::~ObjFile()
{
    if (fd_ >= 0)
        ::close(fd_);
}

shared_ptr<Filesystem> ObjFile::fs()
{
    return fs_.lock();
}

void
ObjFile::handle(FileHandle& fh)
{
    fh.fsid = fs_.lock()->fsid();
    fh.handle.resize(sizeof(FileId));
    *reinterpret_cast<FileId*>(fh.handle.data()) = FileId(meta_.fileid);
}

bool ObjFile::access(const Credential& cred, int accmode)
{
    auto attr = getattr();
    try {
        checkAccess(cred, accmode);
        return true;
    }
    catch (system_error&) {
        return false;
    }
}

shared_ptr<Getattr> ObjFile::getattr()
{
    // XXX: defer the call to spaceUsed until Getattr::used is called?
    auto fs = fs_.lock();
    DataKeyType start(fileid(), 0);
    DataKeyType end(fileid(), ~0ull);
    auto used = fs->db()->spaceUsed(fs->dataNS(), start, end);
    return make_shared<ObjGetattr>(meta_, used);
}

void ObjFile::setattr(const Credential& cred, function<void(Setattr*)> cb)
{
    auto oldSize = meta_.attr.size;
    ObjSetattr sattr(cred, meta_);
    cb(&sattr);
    meta_.attr.ctime = getTime();
    if (meta_.attr.size < oldSize) {
        switch (meta_.location.type) {
        case LOC_DB: {
            // If the file is truncated, purge any data after the new size.
            auto blockSize = meta_.location.db().blockSize;
            auto blockMask = blockSize - 1;
            auto fs = fs_.lock();

            DataKeyType start(
                fileid(), (meta_.attr.size + blockMask) & ~blockMask);
            DataKeyType end(fileid(), ~0ull);

            auto iterator = fs->db()->iterator(fs->dataNS());
            auto trans = fs->db()->beginTransaction();
            iterator->seek(start);
            while (iterator->valid(end)) {
                trans->remove(fs->dataNS(), *iterator->key());
                iterator->next();
            }
            fs->db()->commit(move(trans));
            break;
        }
        default:
            // Handle LOC_FILE and LOC_NFS
            break;
        }
    }
    writeMeta();
}

shared_ptr<File> ObjFile::lookup(const Credential& cred, const string& name)
{
    return lookupInternal(cred, name);
}

shared_ptr<File> ObjFile::open(
    const Credential& cred, const string& name, int flags, function<void(Setattr*)> cb)
{
    if (flags & OpenFlags::CREATE) {
        shared_ptr<ObjFile> file;
        try {
            file = lookupInternal(cred, name);
        }
        catch (system_error& e) {
            if (e.code().value() != ENOENT)
                throw;
            return createNewFile(
                cred, PT_REG, name, cb, [this](auto, auto newFile) {
                auto& loc =  newFile->meta_.location;
                loc.set_type(LOC_DB);
                loc.db().blockSize = fs_.lock()->blockSize();
            });
        }
        if (flags & OpenFlags::EXCLUSIVE)
            throw system_error(EEXIST, system_category());
        return file;
    }
    else {
        return lookupInternal(cred, name);
    }
}

void ObjFile::close(const Credential& cred)
{
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

void ObjFile::commit(const Credential& cred)
{
    // XXX: possibly too expensive
    fs_.lock()->db()->flush();
}

string ObjFile::readlink(const Credential& cred)
{
    if (meta_.attr.type != PT_LNK)
        throw system_error(EINVAL, system_category());
    checkAccess(cred, AccessFlags::READ);

    assert(meta_.location.type == LOC_EMBEDDED);
    meta_.attr.ctime = meta_.attr.atime = getTime();
    writeMeta();
    auto& val = meta_.location.embedded().data;
    return string(reinterpret_cast<const char*>(val.data()), val.size());
}

std::shared_ptr<oncrpc::Buffer> ObjFile::read(
    const Credential& cred, uint64_t offset, uint32_t len, bool& eof)
{
    checkAccess(cred, AccessFlags::READ);

    meta_.attr.ctime = meta_.attr.atime = getTime();
    writeMeta();

    switch (meta_.location.type) {
    case LOC_EMBEDDED: {
        const auto& data = meta_.location.embedded().data;
        if (offset >= data.size()) {
            eof = true;
            return make_shared<oncrpc::Buffer>(0);
        }
        if (offset + len > data.size()) {
            len = data.size() - offset;
        }
        auto res = make_shared<oncrpc::Buffer>(len);
        copy_n(&data[offset], len, res->data());
        eof = false;
        return res;
    }
    case LOC_FILE: {
        if (fd_ < 0) {
            fd_ = ::open(meta_.location.file().filename.c_str(), O_RDWR);
            if (fd_ < 0 && errno == EACCES)
                fd_ = ::open(meta_.location.file().filename.c_str(), O_RDONLY);
            if (fd_ < 0)
                throw system_error(errno, system_category());
        }
        auto res = make_shared<oncrpc::Buffer>(len);
        auto n = ::pread(fd_, res->data(), len, offset);
        if (n < 0)
            throw system_error(errno, system_category());
        if (n < len)
            res = make_shared<oncrpc::Buffer>(res, 0, n);
        return res;
    }
    case LOC_DB: {
        auto blockSize = meta_.location.db().blockSize;
        auto bn = offset / blockSize;
        auto boff = offset % blockSize;
        auto fs = fs_.lock();
        eof = false;
        if (offset >= meta_.attr.size) {
            eof = true;
            return make_shared<oncrpc::Buffer>(0);
        }
        if (offset + len >= meta_.attr.size) {
            eof = true;
            len = meta_.attr.size - offset;
        }

        // Read one block at a time and copy out to buffer
        auto res = make_shared<oncrpc::Buffer>(len);
        for (int i = 0; i < len; ) {
            auto off = bn * blockSize;
            try {
                // If the block exists copy out to buffer
                auto block = fs->db()->get(
                    fs->dataNS(), DataKeyType(fileid(), off));
                auto blen = block->size() - boff;
                if (i + blen > len) {
                    blen = len - i;
                }
                copy_n(block->data() + boff, blen, res->data() + i);
                i += blen;
            }
            catch (system_error&) {
                // otherwise copy zeros
                auto blen = blockSize - boff;
                if (i + blen > len) {
                    blen = len - i;
                }
                fill_n(res->data() + i, blen, 0);
                i += blen;
            }

            boff = 0;
            bn++;
        }
        return res;
    }
    default:
        assert(false);
    }
}

uint32_t ObjFile::write(
    const Credential& cred, uint64_t offset,
    std::shared_ptr<oncrpc::Buffer> data)
{
    checkAccess(cred, AccessFlags::WRITE);

    switch (meta_.location.type) {
    case LOC_DB: {
        auto blockSize = meta_.location.db().blockSize;
        auto bn = offset / blockSize;
        auto boff = offset % blockSize;
        auto len = data->size();
        auto fs = fs_.lock();
        auto trans = fs->db()->beginTransaction();
        meta_.attr.ctime = meta_.attr.mtime = getTime();
        if (offset + len > meta_.attr.size) {
            meta_.attr.size = offset + len;
        }
        writeMeta(trans.get());

        // Write one block at a time, merging if necessary
        for (int i = 0; i < len; ) {
            auto off = bn * blockSize;
            auto blen = blockSize - boff;
            if (i + blen > len)
                blen = len - i;
            // If we need to merge, read the existing block
            unique_ptr<Buffer> block;
            DataKeyType key(fileid(), off);
            if (boff > 0 ||
                (blen < blockSize && off + blen < meta_.attr.size)) {
                try {
                    block = fs->db()->get(fs->dataNS(), key);
                }
                catch (system_error&) {
                    block = make_unique<Buffer>(blockSize);
                    fill_n(block->data(), blockSize, 0);
                }
                copy_n(data->data() + i, blen, block->data() + boff);
                trans->put(fs->dataNS(), DataKeyType(fileid(), off), *block);
            }
            else {
                shared_ptr<Buffer> block;
                if (blen == blockSize) {
                    block = make_shared<Buffer>(data, i, i + blen);
                }
                else {
                    block = make_shared<Buffer>(blockSize);
                    copy_n(data->data() + i, blen, block->data());
                    fill_n(block->data() + blen, blockSize - blen, 0);
                }
                trans->put(fs->dataNS(), DataKeyType(fileid(), off), *block);
            }

            // Set up for the next block - note that only the first block can
            // be at a non-zero offset within the block
            i += blen;
            boff = 0;
            bn++;
        }

        fs->db()->commit(move(trans));
        return len;
    }
    default:
        throw system_error(EINVAL, system_category());
    }
}

shared_ptr<File> ObjFile::mkdir(
    const Credential& cred, const string& name, function<void(Setattr*)> cb)
{
    return createNewFile(
        cred, PT_DIR, name, cb, [this](auto trans, auto newFile) {
        // Add initial directory entries and adjust link counts
        newFile->link(trans, ".", newFile.get(), false);
        newFile->link(trans, "..", this, false);
    });
}

shared_ptr<File> ObjFile::symlink(
    const Credential& cred, const string& name, const string& data,
    function<void(Setattr*)> cb)
{
    return createNewFile(cred, PT_LNK, name, cb,
        [this, data](auto trans, auto newFile) {
            newFile->meta_.attr.size = data.size();
            auto& val = newFile->meta_.location.embedded().data;
            val.resize(data.size());
            copy_n(
                reinterpret_cast<const uint8_t*>(data.data()),
                data.size(), val.data());
        });
}

std::shared_ptr<File> ObjFile::mkfifo(
    const Credential& cred, const std::string& name,
    std::function<void(Setattr*)> cb)
{
    return createNewFile(
        cred, PT_FIFO, name, cb, [](auto trans, auto newFile) {});
}

void ObjFile::remove(const Credential& cred, const string& name)
{
    auto file = lookupInternal(cred, name);
    if (file->meta_.attr.type == PT_DIR)
        throw system_error(EISDIR, system_category());
    checkAccess(cred, AccessFlags::WRITE);
    checkSticky(cred, file.get());

    auto fs = fs_.lock();
    auto trans = fs->db()->beginTransaction();
    unlink(trans.get(), name, file.get(), true);
    fs->db()->commit(move(trans));
}

void ObjFile::rmdir(const Credential& cred, const string& name)
{
    auto file = lookupInternal(cred, name);
    if (file->meta_.attr.type != PT_DIR)
        throw system_error(ENOTDIR, system_category());
    checkAccess(cred, AccessFlags::WRITE);
    checkSticky(cred, file.get());

    auto fs = fs_.lock();
    auto trans = fs->db()->beginTransaction();
    unlink(trans.get(), name, file.get(), true);
    fs->db()->commit(move(trans));
}

void ObjFile::rename(
    const Credential& cred, const string& toName,
    shared_ptr<File> fromDir, const string& fromName)
{
    shared_ptr<ObjFile> tofile;
    try {
        tofile = lookupInternal(cred, toName);
    }
    catch (system_error& e) {
        if (e.code().value() != ENOENT)
            throw;
    }

    auto ofrom = dynamic_cast<ObjFile*>(fromDir.get());
    if (ofrom == this && fromName == toName)
        return;

    // We need write permission for both directories
    checkAccess(cred, AccessFlags::WRITE);
    ofrom->checkAccess(cred, AccessFlags::WRITE);

    auto file = ofrom->lookupInternal(cred, fromName);
    ofrom->checkSticky(cred, file.get());
    auto fs = fs_.lock();
    auto trans = fs->db()->beginTransaction();
    auto h = fs->directoriesNS();

    if (tofile) {
        // Atomically delete target file if it exists
        VLOG(2) << "rename: target name " << toName << " exists";
        checkSticky(cred, tofile.get());
        unlink(trans.get(), toName, tofile.get(), false);
    }

    // Write directory entries and adjust sizes. We don't call unlink here
    // since it doesn too much work
    link(trans.get(), toName, file.get(), false);
    ofrom->meta_.attr.size--;
    ofrom->meta_.attr.ctime = ofrom->meta_.attr.mtime = getTime();
    file->meta_.attr.nlink--;
    trans->remove(h, DirectoryKeyType(ofrom->fileid(), fromName));

    // Adjust ".." links if necessary
    if (file->meta_.attr.type == PT_DIR && ofrom != this) {
        ofrom->meta_.attr.nlink--;
        meta_.attr.nlink++;
        file->writeDirectoryEntry(trans.get(), "..", fileid());
        file->meta_.attr.ctime = file->meta_.attr.mtime = getTime();
        file->writeMeta(trans.get());
    }

    ofrom->writeMeta(trans.get());
    writeMeta(trans.get());

    fs->db()->commit(move(trans));
}

void ObjFile::link(
    const Credential& cred, const std::string& name, std::shared_ptr<File> file)
{
    shared_ptr<File> old;
    try {
        // If the entry exists, throw an error
        old = lookupInternal(cred, name);
    }
    catch (system_error& e) {
        if (e.code().value() != ENOENT)
            throw;
    }
    if (old)
        throw system_error(EEXIST, system_category());

    checkAccess(cred, AccessFlags::WRITE);

    auto from = dynamic_cast<ObjFile*>(file.get());
    if (from->meta_.attr.type == PT_DIR)
        throw system_error(EISDIR, system_category());

    auto fs = fs_.lock();
    auto trans = fs->db()->beginTransaction();
    link(trans.get(), name, from, true);
    fs->db()->commit(move(trans));
}

shared_ptr<DirectoryIterator> ObjFile::readdir(
    const Credential& cred, uint64_t seek)
{
    if (meta_.attr.type != PT_DIR)
        throw system_error(ENOTDIR, system_category());
    checkAccess(cred, AccessFlags::READ);
    meta_.attr.ctime = meta_.attr.atime = getTime();
    writeMeta();
    return make_shared<ObjDirectoryIterator>(
        fs_.lock(), FileId(meta_.fileid), seek);
}

std::shared_ptr<Fsattr> ObjFile::fsstat(const Credential& cred)
{
    checkAccess(cred, AccessFlags::WRITE);
    return make_shared<ObjFsattr>();
}

shared_ptr<ObjFile> ObjFile::lookupInternal(
    const Credential& cred, const string& name)
{
    if (name.size() > OBJFS_NAME_MAX)
        throw system_error(ENAMETOOLONG, system_category());
    if (meta_.attr.type != PT_DIR)
        throw system_error(ENOTDIR, system_category());
    checkAccess(cred, AccessFlags::EXECUTE);
    auto fs = fs_.lock();
    DirectoryKeyType key(fileid(), name);
    auto val = fs->db()->get(fs->directoriesNS(), key);
    DirectoryEntry entry;
    oncrpc::XdrMemory xm(val->data(), val->size());
    xdr(entry, static_cast<oncrpc::XdrSource*>(&xm));
    return fs->find(FileId(entry.fileid));
}

void ObjFile::writeMeta()
{
    auto fs = fs_.lock();
    auto trans = fs->db()->beginTransaction();
    writeMeta(trans.get());
    fs->db()->commit(move(trans));
}

void ObjFile::writeMeta(Transaction* trans)
{
    oncrpc::XdrMemory xm(512);
    xdr(meta_, static_cast<oncrpc::XdrSink*>(&xm));
    auto fs = fs_.lock();
    trans->put(
        fs->defaultNS(),
        KeyType(fileid()),
        oncrpc::Buffer(xm.writePos(), xm.buf()));
}

void ObjFile::writeDirectoryEntry(
    Transaction* trans, const string& name, FileId id)
{
    oncrpc::XdrMemory xm(512);
    DirectoryKeyType key(fileid(), name);
    DirectoryEntry entry;
    entry.fileid = id;
    xdr(entry, static_cast<oncrpc::XdrSink*>(&xm));
    auto fs = fs_.lock();
    auto h = fs->directoriesNS();
    trans->put(h, key, oncrpc::Buffer(xm.writePos(), xm.buf()));
}

void ObjFile::link(
    Transaction* trans, const string& name, ObjFile* file, bool saveMeta)
{
    // We use attr.size for the number of entries in the directory
    meta_.attr.size++;
    file->meta_.attr.nlink++;
    writeDirectoryEntry(trans, name, file->fileid());
    meta_.attr.ctime = meta_.attr.mtime = getTime();
    if (saveMeta) {
        writeMeta(trans);
        file->writeMeta(trans);
    }
}

void ObjFile::unlink(
    Transaction* trans, const string& name, ObjFile* file, bool saveMeta)
{
    auto id = file->fileid();
    auto fs = fs_.lock();
    auto h = fs->directoriesNS();
    if (file->meta_.attr.type == PT_DIR) {
        if (file->meta_.attr.size != 2)
            throw system_error(ENOTEMPTY, system_category());
        VLOG(2) << "deleting directory fileid: " << id;
        trans->remove(h, DirectoryKeyType(id, "."));
        trans->remove(h, DirectoryKeyType(id, ".."));
        trans->remove(fs->defaultNS(), KeyType(id));
        meta_.attr.nlink--;
        assert(meta_.attr.size > 0);
    }
    else {
        file->meta_.attr.nlink--;
        if (file->meta_.attr.nlink > 0)
            file->writeMeta(trans);
        else {
            VLOG(2) << "deleting fileid: " << id;
            // Purge file data
            switch (file->meta_.location.type) {
            case LOC_DB: {
                auto iterator = fs->db()->iterator(fs->dataNS());
                DataKeyType start(file->fileid(), 0);
                DataKeyType end(file->fileid(), ~0ull);
                iterator->seek(start);
                while (iterator->valid(end)) {
                    trans->remove(fs->dataNS(), *iterator->key());
                    iterator->next();
                }
                break;
            }
            default:
                // XXX: handle LOC_FILE and LOC_NFS
                break;
            }
            trans->remove(fs->defaultNS(), KeyType(id));
        }
    }
    DirectoryKeyType key(fileid(), name);
    trans->remove(h, key);

    meta_.attr.size--;
    meta_.attr.ctime = meta_.attr.mtime = getTime();
    if (saveMeta) {
        assert(meta_.attr.size >= 2);
        writeMeta(trans);
    }
}

std::shared_ptr<File> ObjFile::createNewFile(
    const Credential& cred,
    PosixType type,
    const std::string& name,
    std::function<void(Setattr*)> attrCb,
    std::function<void(Transaction*, shared_ptr<ObjFile>)> writeCb)
{
    if (name.size() > OBJFS_NAME_MAX)
        throw system_error(ENAMETOOLONG, system_category());

    // If the entry exists, throw an error
    shared_ptr<File> old;
    try {
        old = lookupInternal(cred, name);
    }
    catch (system_error&) {
    }
    if (old)
        throw system_error(EEXIST, system_category());

    checkAccess(cred, AccessFlags::WRITE);

    auto now = getTime();

    auto fs = fs_.lock();

    ObjFileMetaImpl meta;
    auto newfileid = fs->nextId();
    meta.fileid = newfileid;
    meta.attr.type = type;
    meta.attr.mode = 0;
    meta.attr.nlink = 0;
    meta.attr.size = 0;
    meta.attr.uid = cred.uid();
    if (meta_.attr.mode & ModeFlags::SETGID)
        meta.attr.gid = meta_.attr.gid;
    else
        meta.attr.gid = cred.gid();
    meta.attr.atime = now;
    meta.attr.mtime = now;
    meta.attr.ctime = now;
    meta.attr.birthtime = now;

    ObjSetattr sattr(cred, meta);
    attrCb(&sattr);

    // Create the new file and add to our cache
    auto newFile = make_shared<ObjFile>(fs, move(meta));
    fs->add(newFile);

    // Write a single transaction which increments ObjFilesystem::nextId,
    // and our size, writes the new file meta and adds the directory entry
    auto trans = fs->db()->beginTransaction();
    writeCb(trans.get(), newFile);
    fs->writeMeta(trans.get());
    link(trans.get(), name, newFile.get(), true);
    fs->db()->commit(move(trans));

    return newFile;
}

void ObjFile::checkAccess(const Credential& cred, int accmode)
{
    return CheckAccess(
        meta_.attr.uid, meta_.attr.gid, meta_.attr.mode, cred, accmode);
}

void ObjFile::checkSticky(const Credential& cred, ObjFile* file)
{
    if (cred.privileged())
        return;
    if (meta_.attr.mode & ModeFlags::STICKY) {
        if (cred.uid() != file->meta_.attr.uid) {
            throw system_error(EPERM, system_category());
        }
    }
}
