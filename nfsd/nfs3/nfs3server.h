#pragma once

#include <fs++/proto/nfs_prot.h>

namespace nfsd {
namespace nfs3 {

class NfsServer: public filesys::nfs3::NfsProgram3Service
{
public:
    NfsServer(const std::vector<int>& sec);

    // INfsProgram3 overrides
    void dispatch(oncrpc::CallContext&& ctx) override;
    void null() override;
    filesys::nfs3::GETATTR3res getattr(const filesys::nfs3::GETATTR3args& args) override;
    filesys::nfs3::SETATTR3res setattr(const filesys::nfs3::SETATTR3args& args) override;
    filesys::nfs3::LOOKUP3res lookup(const filesys::nfs3::LOOKUP3args& args) override;
    filesys::nfs3::ACCESS3res access(const filesys::nfs3::ACCESS3args& args) override;
    filesys::nfs3::READLINK3res readlink(const filesys::nfs3::READLINK3args& args) override;
    filesys::nfs3::READ3res read(const filesys::nfs3::READ3args& args) override;
    filesys::nfs3::WRITE3res write(const filesys::nfs3::WRITE3args& args) override;
    filesys::nfs3::CREATE3res create(const filesys::nfs3::CREATE3args& args) override;
    filesys::nfs3::MKDIR3res mkdir(const filesys::nfs3::MKDIR3args& args) override;
    filesys::nfs3::SYMLINK3res symlink(const filesys::nfs3::SYMLINK3args& args) override;
    filesys::nfs3::MKNOD3res mknod(const filesys::nfs3::MKNOD3args& args) override;
    filesys::nfs3::REMOVE3res remove(const filesys::nfs3::REMOVE3args& args) override;
    filesys::nfs3::RMDIR3res rmdir(const filesys::nfs3::RMDIR3args& args) override;
    filesys::nfs3::RENAME3res rename(const filesys::nfs3::RENAME3args& args) override;
    filesys::nfs3::LINK3res link(const filesys::nfs3::LINK3args& args) override;
    filesys::nfs3::READDIR3res readdir(const filesys::nfs3::READDIR3args& args) override;
    filesys::nfs3::READDIRPLUS3res readdirplus(const filesys::nfs3::READDIRPLUS3args& args) override;
    filesys::nfs3::FSSTAT3res fsstat(const filesys::nfs3::FSSTAT3args& args) override;
    filesys::nfs3::FSINFO3res fsinfo(const filesys::nfs3::FSINFO3args& args) override;
    filesys::nfs3::PATHCONF3res pathconf(const filesys::nfs3::PATHCONF3args& args) override;
    filesys::nfs3::COMMIT3res commit(const filesys::nfs3::COMMIT3args& args) override;

private:
    std::vector<int> sec_;
};

}
}
