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

#include "nfs3fs.h"

using namespace filesys;
using namespace filesys::nfs3;
using namespace std::chrono;

static auto fromNfsTime(const nfstime3& t)
{
    auto d = seconds(t.seconds) + nanoseconds(t.nseconds);
    return system_clock::time_point(duration_cast<system_clock::duration>(d));
}

static auto toNfsTime(system_clock::time_point time)
{
    auto d = time.time_since_epoch();
    auto sec = duration_cast<seconds>(d);
    auto nsec = duration_cast<nanoseconds>(d) - sec;
    return nfstime3{uint32(sec.count()), uint32(nsec.count())};
}

NfsGetattr::NfsGetattr(const fattr3& attr)
    : attr_(attr)
{
}

FileType NfsGetattr::type() const
{
    switch (attr_.type) {
    case NF3REG:
        return FileType::FILE;
    case NF3DIR:
        return FileType::DIRECTORY;
    case NF3BLK:
        return FileType::BLOCKDEV;
    case NF3CHR:
        return FileType::CHARDEV;
    case NF3LNK:
        return FileType::SYMLINK;
    case NF3SOCK:
        return FileType::SOCKET;
    case NF3FIFO:
        return FileType::FIFO;
    }
    abort();
}

int NfsGetattr::mode() const
{
    return attr_.mode;
}

int NfsGetattr::nlink() const
{
    return attr_.nlink;
}

int NfsGetattr::uid() const
{
    return attr_.uid;
}

int NfsGetattr::gid() const
{
    return attr_.gid;
}

std::uint64_t NfsGetattr::size() const
{
    return attr_.size;
}

std::uint64_t NfsGetattr::used() const
{
    return attr_.used;
}

std::uint32_t NfsGetattr::blockSize() const
{
    return 4096;
}

FileId NfsGetattr::fileid() const
{
    return FileId(attr_.fileid);
}

std::chrono::system_clock::time_point NfsGetattr::mtime() const
{
    return  fromNfsTime(attr_.mtime);
}

std::chrono::system_clock::time_point NfsGetattr::atime() const
{
    return  fromNfsTime(attr_.atime);
}

std::chrono::system_clock::time_point NfsGetattr::ctime() const
{
    return  fromNfsTime(attr_.ctime);
}

std::chrono::system_clock::time_point NfsGetattr::birthtime() const
{
    return  fromNfsTime({0, 0});
}

std::uint64_t NfsGetattr::change() const
{
    return (std::uint64_t(attr_.ctime.seconds) << 32) | attr_.ctime.nseconds;
}

std::uint64_t NfsGetattr::createverf() const
{
    return 0;
}

NfsSetattr::NfsSetattr(sattr3& attr)
    : attr_(attr)
{
    attr_.mode.set_set_it(false);
    attr_.uid.set_set_it(false);
    attr_.gid.set_set_it(false);
    attr_.size.set_set_it(false);
    attr_.mtime.set_set_it(DONT_CHANGE);
    attr_.atime.set_set_it(DONT_CHANGE);
}

void NfsSetattr::setMode(int mode)
{
    attr_.mode.set_set_it(true);
    attr_.mode.mode() = mode;
}

void NfsSetattr::setUid(int uid)
{
    attr_.uid.set_set_it(true);
    attr_.uid.uid() = uid;
}

void NfsSetattr::setGid(int gid)
{
    attr_.gid.set_set_it(true);
    attr_.gid.gid() = gid;
}

void NfsSetattr::setSize(std::uint64_t size)
{
    attr_.size.set_set_it(true);
    attr_.size.size() = size;
}

void NfsSetattr::setMtime(std::chrono::system_clock::time_point mtime)
{
    attr_.mtime.set_set_it(SET_TO_CLIENT_TIME);
    attr_.mtime.mtime() = toNfsTime(mtime);
}

void NfsSetattr::setAtime(std::chrono::system_clock::time_point atime)
{
    attr_.atime.set_set_it(SET_TO_CLIENT_TIME);
    attr_.atime.atime() = toNfsTime(atime);
}

void NfsSetattr::setChange(std::uint64_t change)
{
}

void NfsSetattr::setCreateverf(std::uint64_t verf)
{
}

NfsFsattr::NfsFsattr(const FSSTAT3resok& stat, const PATHCONF3resok& pc)
    : tbytes_(stat.tbytes),
      fbytes_(stat.fbytes),
      abytes_(stat.abytes),
      tfiles_(stat.tfiles),
      ffiles_(stat.ffiles),
      afiles_(stat.afiles),
      linkMax_(pc.linkmax),
      nameMax_(pc.name_max)
{
}
