/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <cassert>
#include <system_error>

#include "pfsfs.h"

using namespace filesys;
using namespace filesys::pfs;
using namespace std;

PfsFile::PfsFile(
    shared_ptr<PfsFilesystem> fs, FileId fileid, shared_ptr<PfsFile> parent)
    : fs_(fs),
      fileid_(fileid),
      ctime_(chrono::system_clock::now()),
      parent_(parent)
{
}

shared_ptr<Filesystem> PfsFile::fs()
{
    return fs_.lock();
}

FileHandle
PfsFile::handle()
{
    auto& fsid = fs_.lock()->fsid();
    FileHandle fh;
    fh.handle.resize(fsid.size() + sizeof(FileId));
    copy(fsid.begin(), fsid.end(), fh.handle.begin());
    oncrpc::XdrMemory xm(
        fh.handle.data() + fsid.size(), sizeof(std::uint64_t));
    xdr(fileid_, static_cast<oncrpc::XdrSink*>(&xm));
    return fh;
}

bool PfsFile::access(const Credential& cred, int accmode)
{
    auto attr = getattr();
    try {
        CheckAccess(0, 0, 0555, cred, accmode);
        return true;
    }
    catch (system_error&) {
        return false;
    }
}

shared_ptr<Getattr> PfsFile::getattr()
{
    return make_shared<PfsGetattr>(fileid_, ctime_);
}

void PfsFile::setattr(const Credential&, function<void(Setattr*)>)
{
    throw system_error(EROFS, system_category());
}

shared_ptr<File> PfsFile::lookup(const Credential&, const string& name)
{
    if (name == ".") {
        return shared_from_this();
    }
    if (name == "..") {
        if (parent_)
            return parent_;
        else
            return shared_from_this();
    }
    return find(name)->checkMount();
}

shared_ptr<OpenFile> PfsFile::open(
    const Credential&, const string&, int, function<void(Setattr*)>)
{
    throw system_error(EISDIR, system_category());
}

std::shared_ptr<OpenFile> PfsFile::open(const Credential&, int)
{
    throw system_error(EISDIR, system_category());
}

string PfsFile::readlink(const Credential&)
{
    throw system_error(EISDIR, system_category());
}

shared_ptr<File> PfsFile::mkdir(
    const Credential&, const string&, function<void(Setattr*)>)
{
    throw system_error(EROFS, system_category());
}

shared_ptr<File> PfsFile::symlink(
    const Credential&, const string&, const string& data,
    function<void(Setattr*)>)
{
    throw system_error(EROFS, system_category());
}

std::shared_ptr<File> PfsFile::mkfifo(
    const Credential&, const std::string&, std::function<void(Setattr*)>)
{
    throw system_error(EROFS, system_category());
}

void PfsFile::remove(const Credential&, const string&)
{
    throw system_error(EROFS, system_category());
}

void PfsFile::rmdir(const Credential&, const string&)
{
    throw system_error(EROFS, system_category());
}

void PfsFile::rename(
    const Credential&, const string&, shared_ptr<File>, const string&)
{
    throw system_error(EROFS, system_category());
}

void PfsFile::link(const Credential&, const std::string&, std::shared_ptr<File>)
{
    throw system_error(EROFS, system_category());
}

shared_ptr<DirectoryIterator> PfsFile::readdir(const Credential&, uint64_t)
{
    return make_shared<PfsDirectoryIterator>(entries_);
}

std::shared_ptr<Fsattr> PfsFile::fsstat(const Credential&)
{
    return make_shared<PfsFsattr>();
}

shared_ptr<PfsFile> PfsFile::find(const string& name)
{
    auto i = entries_.find(name);
    if (i == entries_.end())
        throw system_error(ENOENT, system_category());
    auto p = i->second;
    if (p.expired()) {
        entries_.erase(name);
        throw system_error(ENOENT, system_category());
    }
    return p.lock();
}
