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

#include <filesys/filesys.h>
#include <glog/logging.h>

#include "posixfs.h"

using namespace filesys;
using namespace filesys::posix;
using namespace std::chrono;

static auto fromTimespec(const timespec& t)
{
    auto d = seconds(t.tv_sec) + nanoseconds(t.tv_nsec);
    return system_clock::time_point(duration_cast<system_clock::duration>(d));
}

PosixGetattr::PosixGetattr(const struct ::stat& st)
    : stat_(st)
{
}

FileType PosixGetattr::type() const
{
    if (S_ISREG(stat_.st_mode))
        return FileType::FILE;
    if (S_ISDIR(stat_.st_mode))
        return FileType::DIRECTORY;
    if (S_ISBLK(stat_.st_mode))
        return FileType::BLOCKDEV;
    if (S_ISCHR(stat_.st_mode))
        return FileType::CHARDEV;
    if (S_ISLNK(stat_.st_mode))
        return FileType::SYMLINK;
    if (S_ISSOCK(stat_.st_mode))
        return FileType::SOCKET;
    if (S_ISFIFO(stat_.st_mode))
        return FileType::FIFO;
    return FileType::FILE;
}

int PosixGetattr::mode() const
{
    return stat_.st_mode & 0777;
}

int PosixGetattr::nlink() const
{
    return stat_.st_nlink;
}

int PosixGetattr::uid() const
{
    return stat_.st_uid;
}

int PosixGetattr::gid() const
{
    return stat_.st_gid;
}

std::uint64_t PosixGetattr::size() const
{
    return stat_.st_size;
}

std::uint64_t PosixGetattr::used() const
{
    return stat_.st_blocks * 512;
}

std::uint32_t PosixGetattr::blockSize() const
{
    return stat_.st_blksize;
}

FileId PosixGetattr::fileid() const
{
    return FileId(stat_.st_ino);
}

std::chrono::system_clock::time_point PosixGetattr::mtime() const
{
    return fromTimespec(stat_.st_mtimespec);
}

std::chrono::system_clock::time_point PosixGetattr::atime() const
{
    return fromTimespec(stat_.st_atimespec);
}

std::chrono::system_clock::time_point PosixGetattr::ctime() const
{
    return fromTimespec(stat_.st_ctimespec);
}

std::chrono::system_clock::time_point PosixGetattr::birthtime() const
{
    return fromTimespec(stat_.st_birthtimespec);
}

std::uint64_t PosixGetattr::change() const
{
    auto& ts = stat_.st_ctimespec;
    return (std::uint64_t(ts.tv_sec) << 32) | ts.tv_nsec;
}

std::uint64_t PosixGetattr::createverf() const
{
    auto& ts = stat_.st_atimespec;
    return (std::uint64_t(ts.tv_sec) << 32) | ts.tv_nsec;
}
