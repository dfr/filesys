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

#include "distfs.h"

using namespace filesys;
using namespace filesys::distfs;
using namespace std;

DistFsattr::DistFsattr(
    std::shared_ptr<DistFilesystem> fs,
    std::shared_ptr<Fsattr> backingFsattr)
    : objfs::ObjFsattr(
        std::dynamic_pointer_cast<objfs::ObjFilesystem>(fs), backingFsattr),
      storage_(fs->storage()),
      repairQueueSize_(fs->repairQueueSize())
{
}

size_t DistFsattr::totalSpace() const
{
    return storage_.totalSpace;
}

size_t DistFsattr::freeSpace() const
{
    return storage_.freeSpace;
}

size_t DistFsattr::availSpace() const
{
    return storage_.availSpace;
}

int DistFsattr::repairQueueSize() const
{
    return repairQueueSize_;
}

size_t DistFsattr::totalFiles() const
{
    // We don't have a good way to estimate the limit on number of
    // files but lets just assume that it can't be more than the
    // number of bytes of local free space
    if (backingFsattr_)
        return backingFsattr_->freeSpace();
    else
        return freeSpace();
}
