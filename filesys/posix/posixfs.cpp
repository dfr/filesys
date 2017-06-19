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

#include <iomanip>
#include <sstream>

#include <fcntl.h>
#include <sys/stat.h>

#include <filesys/filesys.h>
#include <rpc++/urlparser.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "posixfs.h"

using namespace filesys;
using namespace filesys::posix;
using namespace std;

PosixFilesystem::PosixFilesystem(const std::string& path)
{
    rootfd_ = ::open(path.size() > 0 ? path.c_str() : ".", O_RDONLY);
    if (rootfd_ < 0)
        throw system_error(errno, system_category());
    struct ::stat st;
    ::fstat(rootfd_, &st);
    rootid_ = FileId(st.st_ino);
    struct ::statfs stfs;
    if (::fstatfs(rootfd_, &stfs) < 0)
        throw system_error(errno, system_category());
    fsid_.resize(sizeof(stfs.f_fsid));
    copy_n(reinterpret_cast<const uint8_t*>(&stfs.f_fsid),
           sizeof(stfs.f_fsid), fsid_.data());

    // XXX: Don't cache too many pieces - we are using select and don't
    // want to have file descriptors greater than 1023
    cache_.setCostLimit(512);
}

shared_ptr<File>
PosixFilesystem::root()
{
    return find(nullptr, "/", rootid_, ::dup(rootfd_));
}

const FilesystemId&
PosixFilesystem::fsid() const
{
    return fsid_;
}

shared_ptr<File>
PosixFilesystem::find(const FileHandle& fh)
{
    // To export posix file systems, we need a database mapping fileids to
    // parent directories
    throw system_error(ESTALE, system_category());
}

std::shared_ptr<PosixFile>
PosixFilesystem::find(
    std::shared_ptr<PosixFile> parent, const std::string& name, int fd)
{
    struct ::stat st;
    ::fstat(fd, &st);
    return find(parent, name, FileId(st.st_ino), fd);
}

shared_ptr<PosixFile>
PosixFilesystem::find(
    std::shared_ptr<PosixFile> parent, const std::string& name,
    FileId fileid, int fd)
{
    return cache_.find(
        fileid,
        [fd](auto) {
            ::close(fd);
        },
        [&](uint64_t id) {
            return make_shared<PosixFile>(
                shared_from_this(), parent, name, id, fd);
        });
}

void
PosixFilesystem::remove(FileId id)
{
    cache_.remove(id);
}

shared_ptr<Filesystem>
PosixFilesystemFactory::mount(const string& url)
{
    oncrpc::UrlParser p(url);
    return make_shared<PosixFilesystem>(p.path);
};

void filesys::posix::init(FilesystemManager* fsman)
{
    fsman->add(make_shared<PosixFilesystemFactory>());
}
