#pragma once

#include <fs++/proto/nfs_prot.h>

namespace nfsd {
namespace nfs3 {

class NfsServer: public filesys::nfs::NfsProgram3Service
{
public:
    NfsServer(const std::vector<int>& sec);

    // INfsProgram3 overrides
    void dispatch(oncrpc::CallContext&& ctx) override;
    void null() override;
    filesys::nfs::GETATTR3res getattr(const filesys::nfs::GETATTR3args& args) override;
    filesys::nfs::SETATTR3res setattr(const filesys::nfs::SETATTR3args& args) override;
    filesys::nfs::LOOKUP3res lookup(const filesys::nfs::LOOKUP3args& args) override;
    filesys::nfs::ACCESS3res access(const filesys::nfs::ACCESS3args& args) override;
    filesys::nfs::READLINK3res readlink(const filesys::nfs::READLINK3args& args) override;
    filesys::nfs::READ3res read(const filesys::nfs::READ3args& args) override;
    filesys::nfs::WRITE3res write(const filesys::nfs::WRITE3args& args) override;
    filesys::nfs::CREATE3res create(const filesys::nfs::CREATE3args& args) override;
    filesys::nfs::MKDIR3res mkdir(const filesys::nfs::MKDIR3args& args) override;
    filesys::nfs::SYMLINK3res symlink(const filesys::nfs::SYMLINK3args& args) override;
    filesys::nfs::MKNOD3res mknod(const filesys::nfs::MKNOD3args& args) override;
    filesys::nfs::REMOVE3res remove(const filesys::nfs::REMOVE3args& args) override;
    filesys::nfs::RMDIR3res rmdir(const filesys::nfs::RMDIR3args& args) override;
    filesys::nfs::RENAME3res rename(const filesys::nfs::RENAME3args& args) override;
    filesys::nfs::LINK3res link(const filesys::nfs::LINK3args& args) override;
    filesys::nfs::READDIR3res readdir(const filesys::nfs::READDIR3args& args) override;
    filesys::nfs::READDIRPLUS3res readdirplus(const filesys::nfs::READDIRPLUS3args& args) override;
    filesys::nfs::FSSTAT3res fsstat(const filesys::nfs::FSSTAT3args& args) override;
    filesys::nfs::FSINFO3res fsinfo(const filesys::nfs::FSINFO3args& args) override;
    filesys::nfs::PATHCONF3res pathconf(const filesys::nfs::PATHCONF3args& args) override;
    filesys::nfs::COMMIT3res commit(const filesys::nfs::COMMIT3args& args) override;

private:
    std::vector<int> sec_;
};

}
}
