/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
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
