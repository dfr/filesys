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
#include <chrono>
#include <system_error>
#include <glog/logging.h>

#include "objfs.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace std;
using namespace std::chrono;

FileType ObjGetattr::type() const
{
    switch (attr_.type) {
    case PT_REG:
        return FileType::FILE;
    case PT_DIR:
        return FileType::DIRECTORY;
    case PT_BLK:
        return FileType::BLOCKDEV;
    case PT_CHR:
        return FileType::CHARDEV;
    case PT_LNK:
        return FileType::SYMLINK;
    case PT_SOCK:
        return FileType::SOCKET;
    case PT_FIFO:
        return FileType::FIFO;
    }
    abort();
}

int ObjGetattr::mode() const
{
    return attr_.mode;
}

int ObjGetattr::nlink() const
{
    return attr_.nlink;
}

int ObjGetattr::uid() const
{
    return attr_.uid;
}

int ObjGetattr::gid() const
{
    return attr_.gid;
}

std::uint64_t ObjGetattr::size() const
{
    return attr_.size;
}

std::uint64_t ObjGetattr::used() const
{
    return used_();
}

std::uint32_t ObjGetattr::blockSize() const
{
    return blockSize_;
}

FileId ObjGetattr::fileid() const
{
    return fileid_;
}

std::chrono::system_clock::time_point ObjGetattr::mtime() const
{
    return system_clock::time_point(
        duration_cast<system_clock::duration>(nanoseconds(attr_.mtime)));
}

std::chrono::system_clock::time_point ObjGetattr::atime() const
{
    return system_clock::time_point(
        duration_cast<system_clock::duration>(nanoseconds(attr_.atime)));
}

std::chrono::system_clock::time_point ObjGetattr::ctime() const
{
    return system_clock::time_point(
        duration_cast<system_clock::duration>(nanoseconds(attr_.ctime)));
}

std::chrono::system_clock::time_point ObjGetattr::birthtime() const
{
    return system_clock::time_point(
        duration_cast<system_clock::duration>(
            nanoseconds(attr_.birthtime)));
}

std::uint64_t ObjGetattr::change() const
{
    return attr_.ctime;
}

std::uint64_t ObjGetattr::createverf() const
{
    return attr_.atime;
}

void ObjSetattr::setMode(int mode)
{
    if (cred_.uid() != attr_.uid && !cred_.privileged()) {
        VLOG(1) << "setMode failed: cred uid: " << cred_.uid()
                << ", file uid: " << attr_.uid;
        throw system_error(EPERM, system_category());
    }
    attr_.mode = mode;
}

void ObjSetattr::setUid(int uid)
{
    if (uint32_t(uid) != attr_.uid && !cred_.privileged()) {
        VLOG(1) << "setUid failed: cred uid: " << cred_.uid()
                << ", file uid: " << attr_.uid
                << ", requested uid: " << uid;
        throw system_error(EPERM, system_category());
    }
    attr_.uid = uid;
}

void ObjSetattr::setGid(int gid)
{
    // If the cred matches the file owner and contains the requested group,
    // or the cred is privileged, we can change the file group
    if ((cred_.uid() == attr_.uid && cred_.hasgroup(gid))
        || cred_.privileged()) {
        attr_.gid = gid;
    }
    else {
        VLOG(1) << "setGid failed: cred uid: " << cred_.uid()
                << ", file uid: " << attr_.uid
                << ", requested gid: " << gid;
        throw system_error(EPERM, system_category());
    }
}

void ObjSetattr::setSize(std::uint64_t size)
{
    // FreeBSD allows set size if cred uid is the same as file uid and
    // iozone seems to expect this
    if (cred_.uid() != attr_.uid) {
        CheckAccess(
            attr_.uid, attr_.gid, attr_.mode,
            cred_, AccessFlags::WRITE);
    }
    attr_.size = size;
}

void ObjSetattr::setMtime(std::chrono::system_clock::time_point mtime)
{
    // The file owner can change the times unconditionally
    if (attr_.uid != cred_.uid()) {
        CheckAccess(
            attr_.uid, attr_.gid, attr_.mode,
            cred_, AccessFlags::WRITE);
    }
    attr_.mtime =
        duration_cast<nanoseconds>(mtime.time_since_epoch()).count();
}

void ObjSetattr::setAtime(std::chrono::system_clock::time_point atime)
{
    // The file owner can change the times unconditionally
    if (attr_.uid != cred_.uid()) {
        CheckAccess(
            attr_.uid, attr_.gid, attr_.mode,
            cred_, AccessFlags::WRITE);
    }
    attr_.atime =
        duration_cast<nanoseconds>(atime.time_since_epoch()).count();
}

void ObjSetattr::setChange(std::uint64_t change)
{
    // The file owner can change the times unconditionally
    if (attr_.uid != cred_.uid()) {
        CheckAccess(
            attr_.uid, attr_.gid, attr_.mode,
            cred_, AccessFlags::WRITE);
    }
    // Enforce monotonicity
    if (change > attr_.ctime)
        attr_.ctime = change;
}

void ObjSetattr::setCreateverf(std::uint64_t verf)
{
    // The file owner can change the times unconditionally
    if (attr_.uid != cred_.uid()) {
        CheckAccess(
            attr_.uid, attr_.gid, attr_.mode,
            cred_, AccessFlags::WRITE);
    }
    attr_.atime = verf;
}

ObjFsattr::ObjFsattr(
    std::shared_ptr<ObjFilesystem> fs, std::shared_ptr<Fsattr> backingFsattr)
    : fs_(fs),
      backingFsattr_(backingFsattr)
{
}

size_t ObjFsattr::totalSpace() const
{
    if (backingFsattr_)
        return backingFsattr_->totalSpace();
    else
        return 0;
}

size_t ObjFsattr::freeSpace() const
{
    if (backingFsattr_)
        return backingFsattr_->freeSpace();
    else
        return 0;
}

size_t ObjFsattr::availSpace() const
{
    if (backingFsattr_)
        return backingFsattr_->availSpace();
    else
        return 0;
}

size_t ObjFsattr::totalFiles() const
{
    // We don't have a good way to estimate the limit on number of
    // files but lets just assume that it can't be more than the
    // number of bytes of free space
    return freeSpace();
}

size_t ObjFsattr::freeFiles() const
{
    return totalFiles() - fs_->fileCount();
}

size_t ObjFsattr::availFiles() const
{
    return freeFiles();
}

int ObjFsattr::linkMax() const
{
    return std::numeric_limits<int>::max();
}

int ObjFsattr::nameMax() const
{
    return OBJFS_NAME_MAX;
}

int ObjFsattr::repairQueueSize() const
{
    return 0;
}
