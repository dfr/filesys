#pragma once

#include <fs++/proto/mount.h>

namespace nfsd {
namespace nfs3 {

class MountServer: public filesys::nfs::Mountprog3Service
{
public:
    MountServer(const std::vector<int>& sec);

    // IMountprog3 overrides
    void null() override;
    filesys::nfs::mountres3 mnt(const filesys::nfs::dirpath& dir) override;
    filesys::nfs::mountlist dump() override;
    void umnt(const filesys::nfs::dirpath& dir) override;
    void umntall() override;
    filesys::nfs::exports listexports() override;

private:
    std::vector<int> sec_;
};

}
}
