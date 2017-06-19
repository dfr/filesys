/*-
 * Copyright (c) 2016-present Doug Rabson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
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
