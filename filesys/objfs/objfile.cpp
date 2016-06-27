/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <cassert>
#include <system_error>
#include <fcntl.h>

#include <glog/logging.h>

#include "objfs.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace keyval;
using namespace std;
using namespace chrono;

ObjFile::ObjFile(shared_ptr<ObjFilesystem> fs, FileId fileid)
    : fs_(fs)
{
    KeyType key(fileid);
    auto buf = fs->defaultNS()->get(key);
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
}

shared_ptr<Filesystem> ObjFile::fs()
{
    return fs_.lock();
}

FileHandle
ObjFile::handle()
{
    auto& fsid = fs_.lock()->fsid();
    FileHandle fh;
    fh.handle.resize(fsid.size() + sizeof(FileId));
    copy(fsid.begin(), fsid.end(), fh.handle.begin());
    oncrpc::XdrMemory xm(
        fh.handle.data() + fsid.size(), sizeof(std::uint64_t));
    xdr(meta_.fileid, static_cast<oncrpc::XdrSink*>(&xm));
    return fh;
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
    auto used = [this]() {
        unique_lock<mutex> lock(mutex_);
        auto fs = fs_.lock();
        DataKeyType start(fileid(), 0);
        DataKeyType end(fileid(), ~0ull);
        return fs->dataNS()->spaceUsed(start, end);
    };
    return make_shared<ObjGetattr>(
        fileid(), meta_.attr, meta_.blockSize, used);
}

void ObjFile::setattr(const Credential& cred, function<void(Setattr*)> cb)
{
    unique_lock<mutex> lock(mutex_);
    auto oldSize = meta_.attr.size;
    ObjSetattr sattr(cred, meta_.attr);
    cb(&sattr);
    meta_.attr.ctime = getTime();
    auto fs = fs_.lock();
    auto trans = fs->db()->beginTransaction();
    if (meta_.attr.size != oldSize) {
        // Purge any data after the new size.
        truncate(cred, trans.get(), meta_.attr.size);
    }
    writeMeta(trans.get());
    fs->db()->commit(move(trans));
}

shared_ptr<File> ObjFile::lookup(const Credential& cred, const string& name)
{
    unique_lock<mutex> lock(mutex_);
    checkAccess(cred, AccessFlags::EXECUTE);
    return lookupInternal(lock, cred, name);
}

shared_ptr<OpenFile> ObjFile::open(
    const Credential& cred, const string& name, int flags, function<void(Setattr*)> cb)
{
    unique_lock<mutex> lock(mutex_);
    shared_ptr<ObjFile> file;
    bool created = false;
    checkAccess(cred, AccessFlags::EXECUTE);
    if (flags & OpenFlags::CREATE) {
        try {
            file = lookupInternal(lock, cred, name);
        }
        catch (system_error& e) {
            if (e.code().value() != ENOENT)
                throw;
            file = createNewFile(
                lock, cred, PT_REG, name, cb, [](auto, auto){});
            created = true;
        }
        if ((flags & OpenFlags::EXCLUSIVE) && !created)
            throw system_error(EEXIST, system_category());
    }
    else {
        file = lookupInternal(lock, cred, name);
    }

    if (!created) {
        int accmode = 0;
        if (flags & OpenFlags::READ)
            accmode |= AccessFlags::READ;
        if (flags & OpenFlags::WRITE)
            accmode |= AccessFlags::WRITE;
        file->checkAccess(cred, accmode);
    }

    if (!created && (flags & OpenFlags::TRUNCATE)) {
        if (file->meta_.attr.size > 0) {
            unique_lock<mutex> lock(file->mutex_);
            file->meta_.attr.size = 0;
            file->meta_.attr.ctime = getTime();
            auto fs = fs_.lock();
            auto trans = fs->db()->beginTransaction();
            // Purge file contents
            file->truncate(cred, trans.get(), 0);
            file->writeMeta(trans.get());
            fs->db()->commit(move(trans));
        }
    }
    return fs_.lock()->makeNewOpenFile(cred, file, flags);
}

shared_ptr<OpenFile> ObjFile::open(const Credential& cred, int flags)
{
    int accmode = 0;
    if (flags & OpenFlags::READ)
        accmode |= AccessFlags::READ;
    if (flags & OpenFlags::WRITE)
        accmode |= AccessFlags::WRITE;
    checkAccess(cred, accmode);
    return fs_.lock()->makeNewOpenFile(cred, shared_from_this(), flags);
}

string ObjFile::readlink(const Credential& cred)
{
    unique_lock<mutex> lock(mutex_);
    if (meta_.attr.type != PT_LNK)
        throw system_error(EINVAL, system_category());
    checkAccess(cred, AccessFlags::READ);

    updateAccessTime();
    writeMeta();
    auto& val = meta_.extra;
    return string(reinterpret_cast<const char*>(val.data()), val.size());
}

shared_ptr<File> ObjFile::mkdir(
    const Credential& cred, const string& name, function<void(Setattr*)> cb)
{
    unique_lock<mutex> lock(mutex_);
    return createNewFile(
        lock, cred, PT_DIR, name, cb, [this](auto trans, auto newFile) {
        // Add initial directory entries and adjust link counts
        newFile->link(trans, ".", newFile.get(), false);
        newFile->link(trans, "..", this, false);
    });
}

shared_ptr<File> ObjFile::symlink(
    const Credential& cred, const string& name, const string& data,
    function<void(Setattr*)> cb)
{
    unique_lock<mutex> lock(mutex_);
    return createNewFile(lock, cred, PT_LNK, name, cb,
        [this, data](auto trans, auto newFile) {
            newFile->meta_.attr.size = data.size();
            auto& val = newFile->meta_.extra;
            val.resize(data.size());
            copy_n(
                reinterpret_cast<const uint8_t*>(data.data()),
                data.size(), val.data());
        });
}

shared_ptr<File> ObjFile::mkfifo(
    const Credential& cred, const string& name,
    function<void(Setattr*)> cb)
{
    unique_lock<mutex> lock(mutex_);
    return createNewFile(
        lock, cred, PT_FIFO, name, cb, [](auto trans, auto newFile) {});
}

void ObjFile::remove(const Credential& cred, const string& name)
{
    unique_lock<mutex> lock(mutex_);
    checkAccess(cred, AccessFlags::WRITE|AccessFlags::EXECUTE);

    auto file = lookupInternal(lock, cred, name);
    if (file->meta_.attr.type == PT_DIR)
        throw system_error(EISDIR, system_category());

    checkSticky(cred, file.get());

    auto fs = fs_.lock();
    auto trans = fs->db()->beginTransaction();
    unlink(cred, trans.get(), name, file.get(), true);
    fs->db()->commit(move(trans));
}

void ObjFile::rmdir(const Credential& cred, const string& name)
{
    unique_lock<mutex> lock(mutex_);
    checkAccess(cred, AccessFlags::WRITE|AccessFlags::EXECUTE);

    auto file = lookupInternal(lock, cred, name);
    if (file->meta_.attr.type != PT_DIR)
        throw system_error(ENOTDIR, system_category());

    checkSticky(cred, file.get());

    auto fs = fs_.lock();
    auto trans = fs->db()->beginTransaction();
    unlink(cred, trans.get(), name, file.get(), true);
    fs->db()->commit(move(trans));
}

void ObjFile::rename(
    const Credential& cred, const string& toName,
    shared_ptr<File> fromDir, const string& fromName)
{
    unique_lock<mutex> lock(mutex_);

    shared_ptr<ObjFile> tofile;
    try {
        tofile = lookupInternal(lock, cred, toName);
    }
    catch (system_error& e) {
        if (e.code().value() != ENOENT)
            throw;
    }

    auto ofrom = dynamic_cast<ObjFile*>(fromDir.get());
    if (ofrom == this && fromName == toName)
        return;

    unique_ptr<unique_lock<mutex>> fromlock;
    if (ofrom != this) {
        fromlock = make_unique<unique_lock<mutex>>(ofrom->mutex_);
    }

    // We need write and execute permission for both directories
    checkAccess(cred, AccessFlags::WRITE|AccessFlags::EXECUTE);
    ofrom->checkAccess(cred, AccessFlags::WRITE|AccessFlags::EXECUTE);

    auto file = ofrom->lookupInternal(
        ofrom == this ? lock : *fromlock.get(), cred, fromName);

    // Check that we aren't a descendant of the thing being renamed. Note that
    // we don't care about whether cred has access rights to the path so
    // we use a fake privileged cred iterating up the parent chain.
    // We need to drop the lock for this since lookup needs to lock
    auto fs = fs_.lock();
    if (ofrom != this) {
        auto root = fs->root();
        shared_ptr<File> dir = shared_from_this();
        Credential privcred(0, 0, {}, true);
        lock.unlock();
        while (dir != root) {
            if (dir == file)
                throw system_error(EINVAL, system_category());
            dir = dir->lookup(privcred, "..");
        }
        lock.lock();
    }

    ofrom->checkSticky(cred, file.get());
    auto trans = fs->db()->beginTransaction();
    auto h = fs->directoriesNS();

    if (tofile) {
        // Atomically delete target file if it exists
        VLOG(2) << "rename: target name " << toName << " exists";
        checkSticky(cred, tofile.get());
        unlink(cred, trans.get(), toName, tofile.get(), false);
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
    const Credential& cred, const string& name, shared_ptr<File> file)
{
    unique_lock<mutex> lock(mutex_);
    shared_ptr<File> old;
    try {
        // If the entry exists, throw an error
        old = lookupInternal(lock, cred, name);
    }
    catch (system_error& e) {
        if (e.code().value() != ENOENT)
            throw;
    }
    if (old)
        throw system_error(EEXIST, system_category());

    checkAccess(cred, AccessFlags::WRITE|AccessFlags::EXECUTE);

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
    unique_lock<mutex> lock(mutex_);
    if (meta_.attr.type != PT_DIR)
        throw system_error(ENOTDIR, system_category());
    checkAccess(cred, AccessFlags::READ);
    updateAccessTime();
    writeMeta();
    return make_shared<ObjDirectoryIterator>(
        fs_.lock(), FileId(meta_.fileid), seek);
}

shared_ptr<Fsattr> ObjFile::fsstat(const Credential& cred)
{
    unique_lock<mutex> lock(mutex_);
    checkAccess(cred, AccessFlags::READ);
    return make_shared<ObjFsattr>();
}

shared_ptr<ObjFile> ObjFile::lookupInternal(
    unique_lock<mutex>& lock, const Credential& cred, const string& name)
{
    if (name.size() > OBJFS_NAME_MAX)
        throw system_error(ENAMETOOLONG, system_category());
    if (meta_.attr.type != PT_DIR)
        throw system_error(ENOTDIR, system_category());
    auto fs = fs_.lock();
    DirectoryKeyType key(fileid(), name);
    auto val = fs->directoriesNS()->get(key);
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
    auto buf = make_shared<Buffer>(oncrpc::XdrSizeof(meta_));
    oncrpc::XdrMemory xm(buf->data(), buf->size());
    xdr(meta_, static_cast<oncrpc::XdrSink*>(&xm));
    auto fs = fs_.lock();
    trans->put(fs->defaultNS(), KeyType(fileid()), buf);
}

void ObjFile::writeDirectoryEntry(
    Transaction* trans, const string& name, FileId id)
{
    DirectoryEntry entry;
    auto buf = make_shared<Buffer>(oncrpc::XdrSizeof(entry));
    oncrpc::XdrMemory xm(buf->data(), buf->size());
    DirectoryKeyType key(fileid(), name);
    entry.fileid = id;
    xdr(entry, static_cast<oncrpc::XdrSink*>(&xm));
    auto fs = fs_.lock();
    auto h = fs->directoriesNS();
    trans->put(h, key, buf);
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
    const Credential& cred, Transaction* trans,
    const string& name, ObjFile* file, bool saveMeta)
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
        fs->remove(file->fileid());
    }
    else {
        file->meta_.attr.nlink--;
        if (file->meta_.attr.nlink > 0)
            file->writeMeta(trans);
        else {
            VLOG(2) << "deleting fileid: " << id;
            // Purge file data
            file->truncate(cred, trans, 0);
            trans->remove(fs->defaultNS(), KeyType(id));
            fs->remove(file->fileid());
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

shared_ptr<ObjFile> ObjFile::createNewFile(
    unique_lock<mutex>& lock,
    const Credential& cred,
    PosixType type,
    const string& name,
    function<void(Setattr*)> attrCb,
    function<void(Transaction*, shared_ptr<ObjFile>)> writeCb)
{
    if (name.size() > OBJFS_NAME_MAX)
        throw system_error(ENAMETOOLONG, system_category());

    checkAccess(cred, AccessFlags::WRITE|AccessFlags::EXECUTE);

    // If the entry exists, throw an error
    shared_ptr<File> old;
    try {
        old = lookupInternal(lock, cred, name);
    }
    catch (system_error&) {
    }
    if (old)
        throw system_error(EEXIST, system_category());

    auto now = getTime();

    auto fs = fs_.lock();

    ObjFileMetaImpl meta;
    auto newfileid = fs->nextId();
    meta.fileid = newfileid;
    meta.blockSize = meta_.blockSize;
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

    ObjSetattr sattr(cred, meta.attr);
    attrCb(&sattr);

    // Create the new file and add to our cache
    auto newFile = fs->makeNewFile(move(meta));
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
    VLOG(1) << "checkAccess " << cred.uid() << "/" << cred.gid()
            << ", fileid=" << meta_.fileid
            << ", accmode=" << accmode
            << ": mode=" << oct << meta_.attr.mode << dec
            << ", uid=" << meta_.attr.uid
            << ", gid=" << meta_.attr.gid;
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

uint64_t ObjFile::getTime()
{
    return duration_cast<nanoseconds>(
        fs_.lock()->clock()->now().time_since_epoch()).count();
}

void ObjFile::updateAccessTime()
{
    meta_.attr.ctime = meta_.attr.atime = getTime();
}

void ObjFile::updateModifyTime()
{
    meta_.attr.ctime = meta_.attr.mtime = getTime();
}

void ObjFile::truncate(
    const Credential& cred, Transaction* trans, uint64_t newSize)
{
    // If the file size is reduced, purge any data after the new size.
    auto fs = fs_.lock();
    auto blockSize = meta_.blockSize;
    auto blockMask = blockSize - 1;

    DataKeyType start(
        fileid(), (newSize + blockMask) & ~blockMask);
    DataKeyType end(fileid(), ~0ull);

    auto iterator = fs->dataNS()->iterator(start);
    while (iterator->valid(end)) {
        trans->remove(fs->dataNS(), iterator->key());
        iterator->next();
    }

    // If there is a block containing newSize, zero out the tail of
    // the block so that if the file is extended again in the future,
    // we don't expose old contents
    auto bn = newSize / blockSize;
    auto boff = newSize % blockSize;
    auto off = bn * blockSize;
    DataKeyType key(fileid(), off);
    try {
        auto oldBlock = fs->dataNS()->get(key);
        auto block = make_shared<Buffer>(blockSize);
        assert(boff <= oldBlock->size());
        copy_n(oldBlock->data(), boff, block->data());
        fill_n(block->data() + boff, blockSize - boff, 0);
        trans->put(fs->dataNS(), key, block);
    }
    catch (system_error&) {
    }
}

ObjOpenFile::~ObjOpenFile()
{
    if (fd_ >= 0)
        ::close(fd_);
}

shared_ptr<Buffer> ObjOpenFile::read(
    uint64_t offset, uint32_t len, bool& eof)
{
    unique_lock<mutex> lock(file_->mutex_);
    if ((flags_ & OpenFlags::READ) == 0) {
        throw system_error(EBADF, system_category());
    }
    file_->updateAccessTime();
    file_->writeMeta();
    auto& meta = file_->meta();

    auto fs = file_->fs_.lock();
    auto blockSize = meta.blockSize;
    auto bn = offset / blockSize;
    auto boff = offset % blockSize;
    eof = false;
    if (offset >= meta.attr.size) {
        eof = true;
        return make_shared<oncrpc::Buffer>(0);
    }
    if (offset + len >= meta.attr.size) {
        eof = true;
        len = meta.attr.size - offset;
    }

    // Read one block at a time and copy out to buffer
    auto res = make_shared<oncrpc::Buffer>(len);
    for (int i = 0; i < int(len); ) {
        auto off = bn * blockSize;
        try {
            // If the block exists copy out to buffer
            auto block = fs->dataNS()->get(
                DataKeyType(file_->fileid(), off));
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

uint32_t ObjOpenFile::write(uint64_t offset, shared_ptr<Buffer> data)
{
    auto fs = file_->fs_.lock();
    auto& meta = file_->meta_;
    auto blockSize = meta.blockSize;

    if ((flags_ & OpenFlags::WRITE) == 0) {
        throw system_error(EBADF, system_category());
    }

    auto bn = offset / blockSize;
    auto boff = offset % blockSize;
    auto len = data->size();
    auto trans = fs->db()->beginTransaction();

    // Write one block at a time, merging if necessary. We don't hold
    // the lock to avoid serialising writes - if two threads have
    // conflicting writes, thats their problem.
    for (int i = 0; i < int(len); ) {
        auto off = bn * blockSize;
        auto blen = blockSize - boff;
        if (i + blen > len)
            blen = len - i;
        // If we need to merge, read the existing block
        shared_ptr<Buffer> block;
        DataKeyType key(file_->fileid(), off);
        if (boff > 0 ||
            (blen < blockSize && off + blen < meta.attr.size)) {
            try {
                auto oldBlock = fs->dataNS()->get(key);
                block = make_shared<Buffer>(blockSize);
                copy_n(oldBlock->data(), oldBlock->size(), block->data());
            }
            catch (system_error&) {
                block = make_shared<Buffer>(blockSize);
                fill_n(block->data(), blockSize, 0);
            }
            copy_n(data->data() + i, blen, block->data() + boff);
            trans->put(
                fs->dataNS(), DataKeyType(file_->fileid(), off), block);
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
            trans->put(
                fs->dataNS(), DataKeyType(file_->fileid(), off), block);
        }

        // Set up for the next block - note that only the first block can
        // be at a non-zero offset within the block
        i += blen;
        boff = 0;
        bn++;
    }

    unique_lock<mutex> lock(file_->mutex_);

    needFlush_ = true;
    meta.attr.ctime = meta.attr.mtime = file_->getTime();
    if (offset + len > meta.attr.size) {
        meta.attr.size = offset + len;
    }
    file_->writeMeta(trans.get());

    lock.unlock();

    fs->db()->commit(move(trans));
    return len;
}

void ObjOpenFile::flush()
{
    unique_lock<mutex> lock(file_->mutex_);
    if (needFlush_) {
        needFlush_ = false;
        lock.unlock();
        file_->fs_.lock()->db()->flush();
    }
}
