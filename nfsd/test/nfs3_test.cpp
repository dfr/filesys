/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include <fs++/filesys.h>
#include <fs++/fstests.h>
#include <fs++/proto/mount.h>

#include "filesys/objfs/objfs.h"
#include "filesys/nfs3/nfs3fs.h"
#include "nfsd/nfs3/nfs3.h"

using namespace filesys;
using namespace filesys::nfs3;
using namespace filesys::objfs;
using namespace nfsd::nfs3;
using namespace oncrpc;
using namespace std;

DECLARE_int32(iosize);

class Nfs3TestBase
{
public:
    Nfs3TestBase()
        : fsman_(FilesystemManager::instance())
    {
        clock_ = make_shared<detail::MockClock>();

        // Create a scratch filesystem to 'export'
        objfs_ = make_shared<ObjFilesystem>(keyval::make_memdb(), clock_);
        fsman_.mount("/", objfs_);
        Credential cred(0, 0, {}, true);
        objfs_->root()->setattr(cred, setMode777);
        blockSize_ = objfs_->blockSize();

        // Register mount and nfs services with oncrpc
        svcreg_ = make_shared<ServiceRegistry>();
        chan_ = make_shared<LocalChannel>(svcreg_);
        nfsd::nfs3::init(svcreg_, nullptr, nullptr, {AUTH_SYS}, {});

        // Try to mount our test filesystem
        Mountprog3<oncrpc::SysClient> prog(chan_);
        auto res = prog.mnt("/");
        EXPECT_EQ(MNT3_OK, res.fhs_status);

        // Verify that the returned file handle matches the exported filesystem
        FileHandle fh, resfh;
        fh = objfs_->root()->handle();
        auto& info = res.mountinfo();
        XdrMemory xm(info.fhandle.data(), info.fhandle.size());
        xdr(resfh, static_cast<XdrSource*>(&xm));
        EXPECT_EQ(fh, resfh);

        // Connect an instance of NfsFilesystem using our local channel
        proto_ = make_shared<NfsProgram3<oncrpc::SysClient>>(chan_);
        proto_->client()->set(cred);
        fs_ = make_shared<NfsFilesystem>(
            proto_, clock_, nfs_fh3{move(info.fhandle)});
    }

    ~Nfs3TestBase()
    {
    }

    void setCred(const Credential& cred)
    {
        proto_->client()->set(cred);
    }

    shared_ptr<detail::MockClock> clock_;
    FilesystemManager& fsman_;
    shared_ptr<ObjFilesystem> objfs_;       // objfs backing store
    size_t blockSize_;
    shared_ptr<NfsProgram3<oncrpc::SysClient>> proto_;
    shared_ptr<NfsFilesystem> fs_;          // nfsfs interface to objfs
    shared_ptr<ServiceRegistry> svcreg_;    // oncrpc plumbing
    shared_ptr<Channel> chan_;              // oncrpc plumbing
};

INSTANTIATE_TYPED_TEST_CASE_P(Nfs3Test, FilesystemTest, Nfs3TestBase);

// Extra tests for NFSv3

class Nfs3Test: public Nfs3TestBase, public ::testing::Test
{
public:
};

TEST_F(Nfs3Test, ReaddirLarge)
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
