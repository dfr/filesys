#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include <fs++/filesys.h>
#include <fs++/fstests.h>
#include <fs++/proto/mount.h>

#include "filesys/objfs/objfs.h"
#include "filesys/nfs/nfsfs.h"
#include "nfsd/nfs3/nfs3.h"

using namespace filesys;
using namespace filesys::nfs;
using namespace filesys::objfs;
using namespace nfsd::nfs3;
using namespace oncrpc;
using namespace std;

DEFINE_int32(iosize, 65536, "maximum size for read or write requests");

class Nfs3Test
{
public:
    Nfs3Test()
        : fsman_(FilesystemManager::instance())
    {
        // Create a scratch filesystem to 'export'
        system("rm -rf ./testdb");
        objfs_ = fsman_.mount<ObjFilesystem>("/", "testdb");
        Credential cred(0, 0, {}, true);
        objfs_->root()->setattr(cred, setMode777);
        blockSize_ = objfs_->blockSize();

        // Register mount and nfs services with oncrpc
        svcreg_ = make_shared<ServiceRegistry>();
        chan_ = make_shared<LocalChannel>(svcreg_);
        nfsd::nfs3::init(svcreg_, {AUTH_SYS}, {});

        // Try to mount our test filesystem
        Mountprog3<oncrpc::SysClient> prog(chan_);
        auto res = prog.mnt("/");
        EXPECT_EQ(MNT3_OK, res.fhs_status);

        // Verify that the returned file handle matches the exported filesystem
        FileHandle fh, resfh;
        objfs_->root()->handle(fh);
        auto& info = res.mountinfo();
        XdrMemory xm(info.fhandle.data(), info.fhandle.size());
        xdr(resfh, static_cast<XdrSource*>(&xm));
        EXPECT_EQ(fh, resfh);

        // Connect an instance of NfsFilesystem using our local channel
        proto_ = make_shared<NfsProgram3<oncrpc::SysClient>>(chan_);
        proto_->client()->set(cred);
        auto clock = make_shared<detail::SystemClock>();
        fs_ = make_shared<NfsFilesystem>(
            proto_, clock, nfs_fh3{move(info.fhandle)});
    }

    ~Nfs3Test()
    {
        fsman_.unmountAll();
    }

    void setCred(const Credential& cred)
    {
        proto_->client()->set(cred);
    }

    FilesystemManager& fsman_;
    shared_ptr<ObjFilesystem> objfs_;       // objfs backing store
    size_t blockSize_;
    shared_ptr<NfsProgram3<oncrpc::SysClient>> proto_;
    shared_ptr<NfsFilesystem> fs_;          // nfsfs interface to objfs
    shared_ptr<ServiceRegistry> svcreg_;    // oncrpc plumbing
    shared_ptr<Channel> chan_;              // oncrpc plumbing
};

INSTANTIATE_TYPED_TEST_CASE_P(Nfs3Test, FilesystemTest, Nfs3Test);

// Extra tests for NFSv3

class Nfs3TestExtra: public Nfs3Test, public ::testing::Test
{
public:
};

TEST_F(Nfs3TestExtra, ReaddirLarge)
{
    Credential cred(0, 0, {}, true);
    auto root = fs_->root();
    for (int i = 0; i < 1000; i++)
        root->symlink(cred, to_string(i), "foo", [](auto){});
    int count = 0;
    for (auto iter = root->readdir(cred, 0); iter->valid(); iter->next()) {
        count++;
    }
    EXPECT_EQ(1002, count);
}
