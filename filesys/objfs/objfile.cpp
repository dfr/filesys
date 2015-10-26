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

shared_ptr<Getattr> ObjFile::getattr()
{
    // XXX: defer the call to spaceUsed until Getattr::used is called?
    auto fs = fs_.lock();
    DataKeyType start(fileid(), 0);
    DataKeyType end(fileid(), ~0ull);
    auto used = fs->db()->spaceUsed(fs->dataNS(), start, end);
    return make_shared<ObjGetattr>(meta_, used);
}

void ObjFile::setattr(function<void(Setattr*)> cb)
{
    auto oldSize = meta_.attr.size;
    ObjSetattr sattr(meta_);
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

shared_ptr<File> ObjFile::lookup(const string& name)
{
    auto fs = fs_.lock();
    DirectoryKeyType key(fileid(), name);
    auto val = fs->db()->get(fs->directoriesNS(), key);
    DirectoryEntry entry;
    oncrpc::XdrMemory xm(val->data(), val->size());
    xdr(entry, static_cast<oncrpc::XdrSource*>(&xm));
    return fs->find(FileId(entry.fileid));
}

shared_ptr<File> ObjFile::open(
    const string& name, int flags, function<void(Setattr*)> cb)
{
    if (flags & OpenFlags::CREATE) {
        shared_ptr<File> file;
        try {
            file = lookup(name);
        }
        catch (system_error&) {
            return createNewFile(PT_REG, name, cb, [this](auto, auto newFile) {
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
        return lookup(name);
    }
}

void ObjFile::close()
{
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

void ObjFile::commit()
{
    // XXX: possibly too expensive
    fs_.lock()->db()->flush();
}

string ObjFile::readlink()
{
    if (meta_.attr.type != PT_LNK)
        throw system_error(EINVAL, system_category());

    assert(meta_.location.type == LOC_EMBEDDED);
    auto& val = meta_.location.embedded().data;
    return string(reinterpret_cast<const char*>(val.data()), val.size());
}

std::shared_ptr<oncrpc::Buffer> ObjFile::read(
    uint64_t offset, uint32_t len, bool& eof)
{
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

uint32_t ObjFile::write(uint64_t offset, std::shared_ptr<oncrpc::Buffer> data)
{
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
    const string& name, function<void(Setattr*)> cb)
{
    return createNewFile(PT_DIR, name, cb, [this](auto trans, auto newFile) {
        // Add initial directory entries and adjust link counts
        meta_.attr.nlink++;
        newFile->meta_.attr.nlink++;
        newFile->writeDirectoryEntry(trans, ".", newFile->fileid());
        newFile->writeDirectoryEntry(trans, "..", fileid());
        newFile->meta_.attr.size = 2;
    });
}

shared_ptr<File> ObjFile::symlink(
    const string& name, const string& data,
    function<void(Setattr*)> cb)
{
    return createNewFile(PT_LNK, name, cb,
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
    const std::string& name, std::function<void(Setattr*)> cb)
{
    return createNewFile(PT_FIFO, name, cb, [](auto trans, auto newFile) {});
}

void ObjFile::remove(const string& name)
{
    auto file = lookup(name);
    auto ofile = dynamic_cast<ObjFile*>(file.get());
    if (ofile->meta_.attr.type == PT_DIR)
        throw system_error(EISDIR, system_category());

    auto fs = fs_.lock();
    auto trans = fs->db()->beginTransaction();
    trans->remove(fs->directoriesNS(), DirectoryKeyType(fileid(), name));
    meta_.attr.size--;
    assert(meta_.attr.size >= 2);
    ofile->meta_.attr.nlink--;
    if (ofile->meta_.attr.nlink > 0) {
        ofile->writeMeta(trans.get());
    }
    else {
        // Purge file data
        switch (ofile->meta_.location.type) {
        case LOC_DB: {
            auto iterator = fs->db()->iterator(fs->dataNS());
            DataKeyType start(ofile->fileid(), 0);
            DataKeyType end(ofile->fileid(), ~0ull);
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
        trans->remove(fs->defaultNS(), KeyType(ofile->fileid()));
    }
    fs->db()->commit(move(trans));
}

void ObjFile::rmdir(const string& name)
{
    auto file = lookup(name);
    auto ofile = dynamic_cast<ObjFile*>(file.get());
    if (ofile->meta_.attr.type != PT_DIR)
        throw system_error(ENOTDIR, system_category());
    if (ofile->meta_.attr.size != 2)
        throw system_error(ENOTEMPTY, system_category());
    ofile->meta_.attr.nlink = 0;
    ofile->meta_.attr.size = 0;

    auto fs = fs_.lock();
    auto trans = fs->db()->beginTransaction();
    auto h = fs->directoriesNS();
    trans->remove(h, DirectoryKeyType(fileid(), name));
    trans->remove(h, DirectoryKeyType(ofile->fileid(), "."));
    trans->remove(h, DirectoryKeyType(ofile->fileid(), ".."));
    meta_.attr.nlink--;
    assert(meta_.attr.nlink > 0);
    meta_.attr.size--;
    assert(meta_.attr.size >= 2);
    writeMeta(trans.get());
    trans->remove(fs->defaultNS(), KeyType(ofile->fileid()));
    fs->db()->commit(move(trans));
}

void ObjFile::rename(
    const string& toName, shared_ptr<File> fromDir, const string& fromName)
{
    shared_ptr<File> tofile;
    try {
        tofile = lookup(toName);
    }
    catch (system_error&) {
    }

    auto ofrom = dynamic_cast<ObjFile*>(fromDir.get());
    auto file = fromDir->lookup(fromName);
    auto ofile = dynamic_cast<ObjFile*>(file.get());
    auto fs = fs_.lock();
    auto trans = fs->db()->beginTransaction();
    auto h = fs->directoriesNS();

    if (tofile) {
        // Atomically delete target file if it exists
        VLOG(2) << "rename: target name " << toName << " exists";
        auto otofile = dynamic_cast<ObjFile*>(tofile.get());
        auto id = otofile->fileid();
        if (otofile->meta_.attr.type == PT_DIR) {
            // XXX: refactor with rmdir
            if (otofile->meta_.attr.size != 2)
                throw system_error(ENOTEMPTY, system_category());
            VLOG(2) << "deleting directory fileid: " << id;
            trans->remove(h, DirectoryKeyType(id, "."));
            trans->remove(h, DirectoryKeyType(id, ".."));
            trans->remove(fs->defaultNS(), KeyType(id));
            meta_.attr.nlink--;
        }
        else {
            // XXX: refactor with remove
            otofile->meta_.attr.nlink--;
            if (otofile->meta_.attr.nlink > 0)
                otofile->writeMeta(trans.get());
            else {
                VLOG(2) << "deleting fileid: " << id;
                trans->remove(fs->defaultNS(), KeyType(id));
            }
        }
        meta_.attr.size--;
    }

    // Write directory entries and adjust sizes
    trans->remove(h, DirectoryKeyType(ofrom->fileid(), fromName));
    writeDirectoryEntry(trans.get(), toName, ofile->fileid());
    ofrom->meta_.attr.size--;
    meta_.attr.size++;

    // Adjust ".." links if necessary
    if (ofile->meta_.attr.type == PT_DIR && ofrom != this) {
        ofrom->meta_.attr.nlink--;
        meta_.attr.nlink++;
        ofile->writeDirectoryEntry(trans.get(), "..", fileid());
    }

    ofrom->writeMeta(trans.get());
    writeMeta(trans.get());

    fs->db()->commit(move(trans));
}

void ObjFile::link(const std::string& name, std::shared_ptr<File> file)
{
    try {
        // If the entry exists, throw an error
        lookup(name);
        throw system_error(EEXIST, system_category());
    }
    catch (system_error&) {
    }

    auto fs = fs_.lock();
    auto trans = fs->db()->beginTransaction();

    // XXX set mtime, atime
    auto from = dynamic_cast<ObjFile*>(file.get());
    if (from->meta_.attr.type == PT_DIR)
        throw system_error(EISDIR, system_category());

    writeDirectoryEntry(trans.get(), name, from->fileid());
    from->meta_.attr.nlink++;
    from->writeMeta(trans.get());

    fs->db()->commit(move(trans));
}

shared_ptr<DirectoryIterator> ObjFile::readdir()
{
    if (meta_.attr.type != PT_DIR)
        throw system_error(ENOTDIR, system_category());
    return make_shared<ObjDirectoryIterator>(fs_.lock(), FileId(meta_.fileid));
}

std::shared_ptr<Fsattr> ObjFile::fsstat()
{
    return make_shared<ObjFsattr>();
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
    Transaction* trans, const string& name, FileId entryid)
{
    oncrpc::XdrMemory xm(512);
    DirectoryKeyType key(fileid(), name);
    DirectoryEntry entry;
    entry.fileid = entryid;
    xdr(entry, static_cast<oncrpc::XdrSink*>(&xm));
    auto fs = fs_.lock();
    trans->put(
        fs->directoriesNS(), key,
        oncrpc::Buffer(xm.writePos(), xm.buf()));
}

std::shared_ptr<File> ObjFile::createNewFile(
    PosixType type,
    const std::string& name,
    std::function<void(Setattr*)> attrCb,
    std::function<void(Transaction*, shared_ptr<ObjFile>)> writeCb)
{
    try {
        // If the entry exists, throw an error
        lookup(name);
        throw system_error(EEXIST, system_category());
    }
    catch (system_error&) {
    }

    auto now = getTime();

    auto fs = fs_.lock();

    ObjFileMetaImpl meta;
    auto newfileid = fs->nextId();
    meta.fileid = newfileid;
    meta.attr.type = type;
    meta.attr.mode = 0;
    meta.attr.nlink = 1;
    meta.attr.atime = now;
    meta.attr.mtime = now;
    meta.attr.ctime = now;
    meta.attr.birthtime = now;

    ObjSetattr sattr(meta);
    attrCb(&sattr);

    // Create the new file and add to our cache
    auto newFile = make_shared<ObjFile>(fs, move(meta));
    fs->add(newFile);

    // Write a single transaction which increments ObjFilesystem::nextId,
    // and our nlink, writes the new file meta and add the directory entry
    auto trans = fs->db()->beginTransaction();

    // We use attr.size for the number of entries in the directory
    meta_.attr.size++;
    meta_.attr.ctime = meta_.attr.mtime = getTime();
    writeCb(trans.get(), newFile);
    fs->writeMeta(trans.get());
    writeMeta(trans.get());
    writeDirectoryEntry(trans.get(), name, newfileid);
    newFile->writeMeta(trans.get());

    fs->db()->commit(move(trans));

    return newFile;
}
