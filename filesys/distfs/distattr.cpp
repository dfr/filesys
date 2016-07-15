/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include "distfs.h"

using namespace filesys;
using namespace filesys::distfs;
using namespace std;

DistFsattr::DistFsattr(std::shared_ptr<DistFilesystem> fs)
    : objfs::ObjFsattr(std::dynamic_pointer_cast<objfs::ObjFilesystem>(fs)),
      storage_(fs->storage())
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
