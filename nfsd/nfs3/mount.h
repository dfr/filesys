/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#pragma once

#include "filesys/proto/mount.h"

namespace nfsd {
namespace nfs3 {

class MountServer: public filesys::nfs3::Mountprog3Service
{
public:
    MountServer(
        const std::vector<int>& sec, std::shared_ptr<filesys::Filesystem> fs);

    // IMountprog3 overrides
    void null() override;
    filesys::nfs3::mountres3 mnt(const filesys::nfs3::dirpath& dir) override;
    filesys::nfs3::mountlist dump() override;
    void umnt(const filesys::nfs3::dirpath& dir) override;
    void umntall() override;
    filesys::nfs3::exports listexports() override;

private:
    std::vector<int> sec_;
    std::shared_ptr<filesys::Filesystem> fs_;
};

}
}
