/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
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
