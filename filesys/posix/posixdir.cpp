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

#include <fcntl.h>
#include <system_error>
#include <unistd.h>

#include "posixfs.h"

using namespace filesys;
using namespace filesys::posix;
using namespace std;

PosixDirectoryIterator::PosixDirectoryIterator(
    shared_ptr<PosixFilesystem> fs, shared_ptr<PosixFile> parent)
    : fs_(fs),
      parent_(parent),
      dir_(::fdopendir(::openat(parent->fd(), ".", O_RDONLY, 0)))
{
    if (dir_)
        next_ = ::readdir(dir_);
    else
        next_ = nullptr;
}

PosixDirectoryIterator::~PosixDirectoryIterator()
{
    if (dir_)
        ::closedir(dir_);
}

bool PosixDirectoryIterator::valid() const
{
    return next_ != nullptr;
}

FileId PosixDirectoryIterator::fileid() const
{
    return FileId(next_->d_ino);
}

string PosixDirectoryIterator::name() const
{
    return next_->d_name;
}

shared_ptr<File> PosixDirectoryIterator::file() const
{
    int fd;
    if (next_->d_type == DT_LNK) {
        // We can't open symlinks
        fd = -1;
    }
    else {
        fd = ::openat(parent_->fd(), next_->d_name, O_RDONLY, 0);
        if (fd < 0)
            throw system_error(errno, system_category());
    }
    return fs_->find(parent_, next_->d_name, FileId(next_->d_ino), fd);
}

uint64_t PosixDirectoryIterator::seek() const
{
    return 0;
}

void PosixDirectoryIterator::next()
{
    if (dir_)
        next_ = ::readdir(dir_);
}
