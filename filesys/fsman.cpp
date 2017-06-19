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

#include <system_error>

#include <filesys/filesys.h>
#include <glog/logging.h>

namespace filesys {
namespace nfs { void init(FilesystemManager* fsman); }
namespace posix { void init(FilesystemManager* fsman); }
namespace objfs { void init(FilesystemManager* fsman); }
namespace distfs { void init(FilesystemManager* fsman); }
namespace data { void init(FilesystemManager* fsman); }
}

using namespace filesys;

FilesystemManager::FilesystemManager()
{
    nfs::init(this);
    posix::init(this);
    objfs::init(this);
    distfs::init(this);
    data::init(this);
}

std::shared_ptr<File> FilesystemManager::find(const FileHandle& fh)
{
    // The initial segment of the handle should match the fsid
    for (auto& entry: filesystems_) {
        auto& fs = entry.second;
        auto& fsid = fs->fsid();
        if (equal(fsid.begin(), fsid.end(),
            fh.handle.begin(), fh.handle.begin() + fsid.size()))
            return fs->find(fh);
    }
    throw std::system_error(ESTALE, std::system_category());
}
