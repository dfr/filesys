#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include <fs++/filesys.h>
#include <fs++/fstests.h>
#include <fs++/proto/mount.h>

#include "filesys/distfs/distfs.h"
#include "filesys/datafs/datafs.h"
#include "filesys/nfs4/nfs4fs.h"
#include "nfsd/nfs4/server.h"

using namespace filesys;
using namespace filesys::data;
using namespace filesys::distfs;
using namespace filesys::nfs4;
using namespace filesys::objfs;
using namespace nfsd::nfs4;
using namespace oncrpc;
using namespace std;
using namespace std::chrono;
using namespace std::placeholders;

DECLARE_int32(iosize);
DECLARE_int32(lease_time);

class FakeIdMapper: public IIdMapper
{
public:
    int toUid(const std::string& s) override
    {
        return stoi(s);
    }

    std::string fromUid(int uid) override
    {
        return to_string(uid) + "@TEST";
    }

    int toGid(const std::string& s) override
    {
        return stoi(s);
    }

    std::string fromGid(int uid) override
    {
        return to_string(uid) + "@TEST";
    }
};

class Nfs4TestBase
{
public:
    Nfs4TestBase()
        : fsman_(FilesystemManager::instance())
    {
        clock_ = make_shared<detail::MockClock>();

        // Create a scratch filesystem to 'export'
        mds_ = fsman_.mount<DistFilesystem>(
            "/", keyval::make_memdb(), "", clock_);
        Credential cred(0, 0, {}, true);
        mds_->root()->setattr(cred, setMode777);
        //blockSize_ = mds_->blockSize();
        blockSize_ = 4096;

        for (int i = 0; i < 5; i++) {
            auto ds = make_shared<DataFilesystem>(
                make_shared<ObjFilesystem>(
                    keyval::make_memdb(), clock_));
            ds_.push_back(ds);
            mds_->addDataStore(ds);
        }

        idmapper_ = make_shared<FakeIdMapper>();

        // Register nfs services with oncrpc
        svcreg_ = make_shared<ServiceRegistry>();
        vector<int> sec = {AUTH_SYS};
        svc_ = make_shared<NfsServer>(sec, idmapper_, clock_);
        svcreg_->add(
            NFS4_PROGRAM, NFS_V4,
            std::bind(&NfsServer::dispatch, svc_.get(), _1));

        // Advance the clock past the default grace period
        *clock_ += 121s;

        // Connect an instance of NfsFilesystem using a local channel
        client_ = make_shared<oncrpc::SysClient>(NFS4_PROGRAM, NFS_V4);
        client_->set(cred);
        chan_ = make_shared<LocalChannel>(svcreg_);
        fs_ = make_shared<NfsFilesystem>(
            chan_, client_, clock_, "fs_", idmapper_);
    }

    ~Nfs4TestBase()
    {
        fsman_.unmountAll();
        fs_->unmount();
    }

    void setCred(const Credential& cred)
    {
        client_->set(cred);
    }

    shared_ptr<detail::MockClock> clock_;
    shared_ptr<IIdMapper> idmapper_;
    FilesystemManager& fsman_;
    shared_ptr<DistFilesystem> mds_;        // metadata filesystem
    vector<shared_ptr<DataFilesystem>> ds_; // data filesystems
    size_t blockSize_;
    shared_ptr<NfsFilesystem> fs_;          // nfsfs interface to objfs
    shared_ptr<ServiceRegistry> svcreg_;    // oncrpc plumbing
    shared_ptr<Channel> chan_;              // oncrpc plumbing
    shared_ptr<NfsServer> svc_;
    shared_ptr<SysClient> client_;          // oncrpc plumbing
};

INSTANTIATE_TYPED_TEST_CASE_P(Nfs4Test, FilesystemTest, Nfs4TestBase);

// Extra tests for NFSv4

class Nfs4Test: public Nfs4TestBase, public ::testing::Test
{
public:
    void expectRecall(const stateid4& expectedStateid)
    {
        using namespace std::placeholders;
        svc_->setRecallHook(
            fs_->sessionid(),
            std::bind(&Nfs4Test::recallHook, this, expectedStateid, _1, _2));
    }

    void recallHook(
        const stateid4& expectedStateid,
        const stateid4& stateid,
        const nfs_fh4& fh)
    {
        recalled_ = true;

        EXPECT_EQ(expectedStateid, stateid);

        // Issue the return asynchronously
        recallThread_ = thread([=]() {
            fs_->compound(
                [&](auto& enc)
                {
                    enc.putfh(fh);
                    enc.delegreturn(stateid);
                },
                [](auto& dec)
                {
                    dec.putfh();
                    dec.delegreturn();
                });
        });
    }

    void expectLayoutRecall(const stateid4& expectedStateid)
    {
        using namespace std::placeholders;
        svc_->setLayoutRecallHook(
            fs_->sessionid(),
            std::bind(
                &Nfs4Test::recallLayoutHook, this, expectedStateid,
                _1, _2, _3, _4));
    }

    void recallLayoutHook(
        const stateid4& expectedStateid, layouttype4 type, layoutiomode4 iomode,
        bool changed, const layoutrecall4& recall)
    {
        recalled_ = true;

        // We expect the seqid to be incremented before the recall
        auto id = expectedStateid;
        id.seqid++;

        EXPECT_EQ(LAYOUT4_FLEX_FILES, type);
        EXPECT_EQ(LAYOUTRECALL4_FILE, recall.lor_recalltype);
        EXPECT_EQ(id, recall.lor_layout().lor_stateid);

        // Issue the return asynchronously
        auto fh = recall.lor_layout().lor_fh;
        auto stateid = recall.lor_layout().lor_stateid;
        recallThread_ = thread([=]() {
            fs_->compound(
                [&](auto& enc)
                {
                    enc.putfh(fh);
                    enc.layoutreturn(
                        false, type, iomode,
                        layoutreturn4(
                            LAYOUTRETURN4_FILE,
                            layoutreturn_file4{
                                0, length4(NFS4_UINT64_MAX),
                                stateid}));
                },
                [](auto& dec)
                {
                    dec.putfh();
                    dec.layoutreturn();
                });
        });
    }

    static pair<OPEN4resok, nfs_fh4> open(
        shared_ptr<NfsFilesystem> fs, const char* name, int access, int deny,
        const open_owner4& owner, nfsstat4 expected = NFS4_OK)
    {
        OPEN4resok res;
        nfs_fh4 fh;
        fs->compound(
            [&](auto& enc)
            {
                enc.putrootfh();
                enc.open(
                    0, access, deny, owner,
                    openflag4(OPEN4_NOCREATE),
                    open_claim4(CLAIM_NULL, toUtf8string(name)));
                enc.getfh();
            },
            [&](auto& dec)
            {
                dec.putrootfh();
                res = dec.open();
                fh = dec.getfh().object;
            });
        return make_pair(move(res), fh);
    }

    static void openFail(
        shared_ptr<NfsFilesystem> fs, const char* name, int access, int deny,
        const open_owner4& owner, nfsstat4 expected)
    {
        OPEN4resok res;
        nfs_fh4 fh;
        fs->compound(
            [&](auto& enc)
            {
                enc.putrootfh();
                enc.open(
                    0, access, deny, owner,
                    openflag4(OPEN4_NOCREATE),
                    open_claim4(CLAIM_NULL, toUtf8string(name)));
            },
            [&](auto& dec)
            {
                dec.putrootfh();
                EXPECT_THROW(
                    try {
                        dec.open();
                    }
                    catch (nfsstat4 stat) {
                        EXPECT_EQ(expected, stat);
                        throw;
                    },
                    nfsstat4);
            });
    }

    static pair<OPEN4resok, nfs_fh4> create(
        shared_ptr<NfsFilesystem> fs, const char* name, int access, int deny,
        const open_owner4& owner, fattr4&& attr)
    {
        OPEN4resok res;
        nfs_fh4 fh;
        fs->compound(
            [&](auto& enc)
            {
                enc.putrootfh();
                enc.open(
                    0, access, deny, owner,
                    openflag4(OPEN4_CREATE, createhow4(UNCHECKED4, move(attr))),
                    open_claim4(CLAIM_NULL, toUtf8string(name)));
                enc.getfh();
            },
            [&](auto& dec)
            {
                dec.putrootfh();
                res = dec.open();
                fh = dec.getfh().object;
            });
        return make_pair(move(res), fh);
    }

    static void delegreturn(
        shared_ptr<NfsFilesystem> fs,
        const nfs_fh4& fh, const stateid4& stateid)
    {
        fs->compound(
            [&](auto& enc)
            {
                enc.putfh(fh);
                enc.delegreturn(stateid);
            },
            [](auto& dec)
            {
                dec.putfh();
                dec.delegreturn();
            });
    }

    static void close(
        shared_ptr<NfsFilesystem> fs,
        const nfs_fh4& fh, const stateid4& stateid)
    {
        fs->compound(
            [&](auto& enc)
            {
                enc.putfh(fh);
                enc.close(0, stateid);
            },
            [](auto& dec)
            {
                dec.putfh();
                dec.close();
            });
    }

    bool recalled_ = false;
    thread recallThread_;
};

TEST_F(Nfs4Test, ReaddirLarge)
{
    Credential cred(0, 0, {}, true);
    auto root = fs_->root();
    for (int i = 0; i < 1000; i++)
        root->symlink(cred, to_string(i), "foo", setMode666);
    int count = 0;
    for (auto iter = root->readdir(cred, 0); iter->valid(); iter->next()) {
        count++;
    }
    EXPECT_EQ(1002, count);
}

TEST_F(Nfs4Test, Replay)
{
    nfs_fh4 fh;
    fs_->compound(
        [](auto& enc) {
            enc.putrootfh();
            enc.getfh();
        },
        [&fh](auto& dec) {
            dec.putrootfh();
            fh = move(dec.getfh().object);
        });

    // The previous call will have used slot zero since we are stricly
    // single threaded in this test
    fs_->forceReplay(0);

    fs_->compound(
        [](auto& enc) {
            enc.putrootfh();
            enc.getfh();
        },
        [&fh](auto& dec) {
            dec.putrootfh();
            EXPECT_EQ(fh, dec.getfh().object);
        });
}

TEST_F(Nfs4Test, Sequence)
{
    // A sequence op which is not first in the compound must return an
    // error. Note that NfsFilesystem::compound will supply the first
    // sequence op.
    fs_->compound(
        [this](auto& enc) {
            enc.sequence(fs_->sessionid(), 99, 99, 99, false);
        },
        [](auto& dec) {
            EXPECT_THROW(
                try {
                    dec.sequence();
                }
                catch (nfsstat4 stat) {
                    EXPECT_EQ(NFS4ERR_SEQUENCE_POS, stat);
                    throw;
                },
                nfsstat4);
        });

    // Any op which is not specifically intended to work without a
    // sequence must not be first in the compound
    fs_->compoundNoSequence(
        [](auto& enc) {
            enc.putrootfh();
        },
        [](auto& dec) {
            EXPECT_THROW(
                try {
                    dec.putrootfh();
                }
                catch (nfsstat4 stat) {
                    EXPECT_EQ(NFS4ERR_OP_NOT_IN_SESSION, stat);
                    throw;
                },
                nfsstat4);
        });

    // An operation other than sequence which can start a compound
    // must be the only operation in the compound
    fs_->compoundNoSequence(
        [](auto& enc) {
            enc.destroy_clientid(1234);
            enc.putrootfh();
        },
        [](auto& dec) {
            EXPECT_THROW(
                try {
                    dec.destroy_clientid();
                }
                catch (nfsstat4 stat) {
                    EXPECT_EQ(NFS4ERR_NOT_ONLY_OP, stat);
                    throw;
                },
                nfsstat4);
        });
}

TEST_F(Nfs4Test, ShareReservations)
{
    open_owner4 oo1{ fs_->clientid(), { 1, 0, 0, 0 } };
    open_owner4 oo2{ fs_->clientid(), { 2, 0, 0, 0 } };
    NfsAttr xattr;
    fattr4 attr;

    xattr.mode_ = 0666;
    xattr.attrmask_ += FATTR4_MODE;
    xattr.encode(attr);

    // Create a file with oo1
    OPEN4resok res1;
    nfs_fh4 fh;
    std::tie(res1, fh) = create(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1, move(attr));

    // Attempting to open the same file with oo2 with the same flags
    // should succeed
    OPEN4resok res2;
    std::tie(res2, std::ignore) = open(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo2);

    // Close res2.stateid and try to re-open with
    // OPEN4_SHARE_DENY_READ - this should fail with
    // NFS4ERR_SHARE_DENIED.
    close(fs_, fh, res2.stateid);
    openFail(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_READ,
        oo2,
        NFS4ERR_SHARE_DENIED);

    // Attempting to open with a second client and oo1 should fail
    // since open owners are only unique to a client
    auto fs2 = make_shared<NfsFilesystem>(
        chan_, client_, clock_, "fs2", idmapper_);
    openFail(
        fs2, "foo",
        OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_READ,
        oo1,
        NFS4ERR_SHARE_DENIED);
    fs2->unmount();
    fs2.reset();

    // Cleanup
    close(fs_, fh, res1.stateid);

    // Open again with oo1, this time with a deny reservation
    std::tie(res1, std::ignore) = open(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_WRITE,
        oo1);

    // Try to open with oo2 - this should fail with
    // NFS4ERR_SHARE_DENIED
    openFail(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo2,
        NFS4ERR_SHARE_DENIED);

    // Try to open again for reading with oo2 - this should succeed
    // since oo1 only denied writes
    std::tie(res2, std::ignore) = open(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo2,
        NFS4ERR_SHARE_DENIED);

    // Cleanup
    close(fs_, fh, res1.stateid);
    close(fs_, fh, res2.stateid);
}

TEST_F(Nfs4Test, OpenUpgrade)
{
    open_owner4 oo1{ fs_->clientid(), { 1, 0, 0, 0 } };
    NfsAttr xattr;
    fattr4 attr;
    uint8_t buf[] = {'f', 'o', 'o'};
    auto data = make_shared<Buffer>(3, buf);

    xattr.mode_ = 0666;
    xattr.attrmask_ += FATTR4_MODE;
    xattr.encode(attr);

    // Create a file with oo1
    OPEN4resok res1;
    nfs_fh4 fh;
    std::tie(res1, fh) = create(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1, move(attr));

    // Writes should fail with NFS4ERR_OPENMODE
    fs_->compound(
        [&](auto& enc)
        {
            enc.putfh(fh);
            enc.write(res1.stateid, 0, UNSTABLE4, data);
        },
        [](auto& dec)
        {
            dec.putfh();
            EXPECT_THROW(
                try {
                    dec.write();
                }
                catch (nfsstat4 stat) {
                    EXPECT_EQ(NFS4ERR_OPENMODE, stat);
                    throw;
                },
                nfsstat4);
        });

    // Re-opening with both access should upgrade
    OPEN4resok res2;
    std::tie(res2, std::ignore) = open(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1);

    // The second stateid should have the same other field with an
    // incremented seqid
    EXPECT_EQ(res1.stateid.other, res2.stateid.other);
    EXPECT_EQ(res1.stateid.seqid + 1, res2.stateid.seqid);

    // The write should now succeed
    fs_->compound(
        [&](auto& enc)
        {
            enc.putfh(fh);
            enc.write(res2.stateid, 0, UNSTABLE4, data);
        },
        [](auto& dec)
        {
            dec.putfh();
            dec.write();
        });

    // Trying to use the old stateid should generate an error
    fs_->compound(
        [&](auto& enc)
        {
            enc.putfh(fh);
            enc.close(0, res1.stateid);
        },
        [](auto& dec)
        {
            dec.putfh();
            EXPECT_THROW(
                try {
                    dec.close();
                }
                catch (nfsstat4 stat) {
                    EXPECT_EQ(NFS4ERR_OLD_STATEID, stat);
                    throw;
                },
                nfsstat4);
        });

    // Cleanup
    close(fs_, fh, res2.stateid);
}

TEST_F(Nfs4Test, ExpireIdleClient)
{
    EXPECT_EQ(0, svc_->expireClients());
    *clock_ += seconds(2*FLAGS_lease_time);
    EXPECT_EQ(1, svc_->expireClients());
}

TEST_F(Nfs4Test, ExpireClientWithState)
{
    Credential cred(0, 0, {}, true);
    auto of = fs_->root()->open(
        cred, "foo", OpenFlags::RDWR+OpenFlags::CREATE, setMode666);
    *clock_ += seconds(2*FLAGS_lease_time);
    EXPECT_EQ(0, svc_->expireClients());
    *clock_ += seconds(20*FLAGS_lease_time);
    EXPECT_EQ(1, svc_->expireClients());

    // Force the client to recover
    fs_->compound([](auto&){}, [](auto&){});
}

TEST_F(Nfs4Test, RevokeExpiredState)
{
    Credential cred(0, 0, {}, true);
    auto of = fs_->root()->open(
        cred, "foo",
        OpenFlags::RDWR+OpenFlags::CREATE+OpenFlags::SHLOCK,
        setMode666);
    auto of2 = fs_->root()->open(
        cred, "foo2",
        OpenFlags::RDWR+OpenFlags::CREATE,
        setMode666);
    *clock_ += seconds(2*FLAGS_lease_time);
    EXPECT_EQ(0, svc_->expireClients());

    // Make a second NfsFilesystem to try to open the file and revoke
    // the state entry. The file without a deny reservation should not
    // be revoked since it doesn't conflict
    auto fs2 = make_shared<NfsFilesystem>(
        chan_, client_, clock_, "fs2", idmapper_);
    auto of3 = fs2->root()->open(
        cred, "foo", OpenFlags::RDWR, setMode666);
    auto of4 = fs2->root()->open(
        cred, "foo2", OpenFlags::RDWR, setMode666);
    of3.reset();
    of4.reset();
    fs2->unmount();
    fs2.reset();

    // Force the original client to notice that it has lost state,
    // exercising test_stateid and free_stateid
    fs_->compound([](auto&){}, [](auto&){});

    // Make sure that of2 didn't get revoked
    uint8_t buf[] = {'f', 'o', 'o'};
    auto data = make_shared<Buffer>(3, buf);
    of2->write(0, data);
}

TEST_F(Nfs4Test, ReadDelegation)
{
    open_owner4 oo1{ fs_->clientid(), { 1, 0, 0, 0 } };
    open_owner4 oo2{ fs_->clientid(), { 2, 0, 0, 0 } };
    NfsAttr xattr;
    fattr4 attr;

    xattr.mode_ = 0666;
    xattr.attrmask_ += FATTR4_MODE;
    xattr.encode(attr);

    // Create a file with oo1, requesting a read delegation
    OPEN4resok res1;
    nfs_fh4 fh;
    std::tie(res1, fh) = create(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WANT_READ_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1, move(attr));

    EXPECT_EQ(OPEN_DELEGATE_READ, res1.delegation.delegation_type);

    // Cleanup
    delegreturn(fs_, fh, res1.delegation.read().stateid);
    close(fs_, fh, res1.stateid);

    // Open with oo2 for writing
    OPEN4resok res2;
    std::tie(res2, std::ignore) = open(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_WRITE | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo2);

    // Attempting to get a read delegation with oo1 should fail
    std::tie(res1, std::ignore) = open(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WANT_READ_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1);

    EXPECT_EQ(OPEN_DELEGATE_NONE_EXT, res1.delegation.delegation_type);

    // Cleanup
    close(fs_, fh, res1.stateid);
    close(fs_, fh, res2.stateid);

    // Open with oo2 for reading
    std::tie(res2, std::ignore) = open(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo2);

    // Attempting to get a read delegation with oo1 should succeed
    std::tie(res1, std::ignore) = open(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WANT_READ_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1);

    EXPECT_EQ(OPEN_DELEGATE_READ, res1.delegation.delegation_type);

    // Cleanup
    delegreturn(fs_, fh, res1.delegation.read().stateid);
    close(fs_, fh, res1.stateid);
    close(fs_, fh, res2.stateid);
}

TEST_F(Nfs4Test, WriteDelegation)
{
    open_owner4 oo1{ fs_->clientid(), { 1, 0, 0, 0 } };
    open_owner4 oo2{ fs_->clientid(), { 2, 0, 0, 0 } };
    NfsAttr xattr;
    fattr4 attr;

    xattr.mode_ = 0666;
    xattr.attrmask_ += FATTR4_MODE;
    xattr.encode(attr);

    // Create a file with oo1, requesting a write delegation
    OPEN4resok res1;
    nfs_fh4 fh;
    std::tie(res1, fh) = create(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1, move(attr));

    EXPECT_EQ(OPEN_DELEGATE_WRITE, res1.delegation.delegation_type);

    // Cleanup
    delegreturn(fs_, fh, res1.delegation.write().stateid);
    close(fs_, fh, res1.stateid);

    // Open with oo2 for writing
    OPEN4resok res2;
    std::tie(res2, std::ignore) = open(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_WRITE | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo2);

    // Attempting to get a write delegation with oo1 should fail
    std::tie(res1, std::ignore) = open(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1);

    EXPECT_EQ(OPEN_DELEGATE_NONE_EXT, res1.delegation.delegation_type);

    // Cleanup
    close(fs_, fh, res1.stateid);
    close(fs_, fh, res2.stateid);
}

TEST_F(Nfs4Test, RecallDelegation)
{
    open_owner4 oo1{ fs_->clientid(), { 1, 0, 0, 0 } };
    open_owner4 oo2{ fs_->clientid(), { 2, 0, 0, 0 } };
    NfsAttr xattr;
    fattr4 attr;

    xattr.mode_ = 0666;
    xattr.attrmask_ += FATTR4_MODE;
    xattr.encode(attr);

    // Create a file with oo1, requesting a write delegation
    OPEN4resok res1;
    nfs_fh4 fh;
    std::tie(res1, fh) = create(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1, move(attr));

    EXPECT_EQ(OPEN_DELEGATE_WRITE, res1.delegation.delegation_type);

    expectRecall(res1.delegation.write().stateid);

    // Open with oo2 for writing - should recall the delegation
    OPEN4resok res2;
    std::tie(res2, std::ignore) = open(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_WRITE | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo2);

    EXPECT_EQ(true, recalled_);
    recallThread_.join();

    // Cleanup
    close(fs_, fh, res1.stateid);
    close(fs_, fh, res2.stateid);
}

TEST_F(Nfs4Test, UpgradeDelegation)
{
    open_owner4 oo1{ fs_->clientid(), { 1, 0, 0, 0 } };
    NfsAttr xattr;
    fattr4 attr;

    xattr.mode_ = 0666;
    xattr.attrmask_ += FATTR4_MODE;
    xattr.encode(attr);

    // Create a file with oo1, requesting a read delegation
    OPEN4resok res1;
    nfs_fh4 fh;
    std::tie(res1, fh) = create(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_READ_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1, move(attr));

    EXPECT_EQ(OPEN_DELEGATE_READ, res1.delegation.delegation_type);

    // Open again requesting a write delegation - should upgrade the
    // delegation, incrementing the seqid
    OPEN4resok res2;
    std::tie(res2, std::ignore) = open(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1);

    EXPECT_EQ(res1.stateid.other, res2.stateid.other);
    EXPECT_EQ(OPEN_DELEGATE_WRITE, res2.delegation.delegation_type);
    EXPECT_EQ(res1.delegation.read().stateid.other,
              res2.delegation.write().stateid.other);
    EXPECT_EQ(res1.delegation.read().stateid.seqid + 1,
              res2.delegation.write().stateid.seqid);

    // Cleanup
    delegreturn(fs_, fh, res2.delegation.write().stateid);
    close(fs_, fh, res2.stateid);
}

TEST_F(Nfs4Test, DowngradeDelegation)
{
    open_owner4 oo1{ fs_->clientid(), { 1, 0, 0, 0 } };
    NfsAttr xattr;
    fattr4 attr;

    xattr.mode_ = 0666;
    xattr.attrmask_ += FATTR4_MODE;
    xattr.encode(attr);

    // Create a file with oo1, requesting a write delegation
    OPEN4resok res1;
    nfs_fh4 fh;
    std::tie(res1, fh) = create(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1, move(attr));

    EXPECT_EQ(OPEN_DELEGATE_WRITE, res1.delegation.delegation_type);

    // Open again requesting a read delegation - should downgrade the
    // delegation, incrementing the seqid
    OPEN4resok res2;
    std::tie(res2, std::ignore) = open(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_READ_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1);

    EXPECT_EQ(res1.stateid.other, res2.stateid.other);
    EXPECT_EQ(OPEN_DELEGATE_READ, res2.delegation.delegation_type);
    EXPECT_EQ(res1.delegation.write().stateid.other,
              res2.delegation.read().stateid.other);
    EXPECT_EQ(res1.delegation.write().stateid.seqid + 1,
              res2.delegation.read().stateid.seqid);

    // Cleanup
    delegreturn(fs_, fh, res2.delegation.read().stateid);
    close(fs_, fh, res2.stateid);
}

TEST_F(Nfs4Test, RecallWriteLayout)
{
    open_owner4 oo1{ fs_->clientid(), { 1, 0, 0, 0 } };
    open_owner4 oo2{ fs_->clientid(), { 2, 0, 0, 0 } };
    NfsAttr xattr;
    fattr4 attr;

    xattr.mode_ = 0666;
    xattr.attrmask_ += FATTR4_MODE;
    xattr.encode(attr);

    // Create a file with oo1
    OPEN4resok res1;
    nfs_fh4 fh;
    std::tie(res1, fh) = create(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1, move(attr));

    // Using that stateid, request a write layout
    LAYOUTGET4resok lret;
    fs_->compound(
        [&](auto& enc)
        {
            enc.putfh(fh);
            enc.layoutget(
                false, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW, 0,
                NFS4_UINT64_MAX, NFS4_UINT64_MAX, res1.stateid, 1);
        },
        [&](auto& dec)
        {
            dec.putfh();
            lret = dec.layoutget();
        });

    EXPECT_EQ(1, lret.logr_layout.size());
    EXPECT_EQ(0, lret.logr_layout[0].lo_offset);
    EXPECT_EQ(NFS4_UINT64_MAX, lret.logr_layout[0].lo_length);
    EXPECT_EQ(LAYOUTIOMODE4_RW, lret.logr_layout[0].lo_iomode);

    expectLayoutRecall(lret.logr_stateid);

    // Make a second NfsFilesystem to try to open the file and revoke
    // the layout. We need a second NfsFilesystem so that it has a
    // conflicting clientid
    auto fs2 = make_shared<NfsFilesystem>(
        chan_, client_, clock_, "fs2", idmapper_);

    // Open with oo2 for writing - should recall the delegation
    OPEN4resok res2;
    std::tie(res2, std::ignore) = open(
        fs2, "foo",
        OPEN4_SHARE_ACCESS_WRITE | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo2);

    EXPECT_EQ(true, recalled_);
    recallThread_.join();

    // Cleanup
    close(fs_, fh, res1.stateid);
    close(fs2, fh, res2.stateid);
}

TEST_F(Nfs4Test, RecallReadLayout)
{
    // Create a file and write some data to it - we can't get a read layout
    // to an empty file.
    Credential cred(0, 0, {}, true);
    auto of = fs_->root()->open(
        cred, "foo", OpenFlags::RDWR+OpenFlags::CREATE, setMode666);
    uint8_t buf[] = {'f', 'o', 'o'};
    auto data = make_shared<Buffer>(3, buf);
    of->write(0, data);
    of.reset();

    open_owner4 oo1{ fs_->clientid(), { 1, 0, 0, 0 } };
    open_owner4 oo2{ fs_->clientid(), { 2, 0, 0, 0 } };
    NfsAttr xattr;
    fattr4 attr;

    xattr.mode_ = 0666;
    xattr.attrmask_ += FATTR4_MODE;
    xattr.encode(attr);

    // Create a file with oo1
    OPEN4resok res1;
    nfs_fh4 fh;
    std::tie(res1, fh) = create(
        fs_, "foo",
        OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo1, move(attr));

    // Using that stateid, request a read layout
    LAYOUTGET4resok lret;
    fs_->compound(
        [&](auto& enc)
        {
            enc.putfh(fh);
            enc.layoutget(
                false, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ, 0,
                NFS4_UINT64_MAX, NFS4_UINT64_MAX, res1.stateid, 1);
        },
        [&](auto& dec)
        {
            dec.putfh();
            lret = dec.layoutget();
        });

    EXPECT_EQ(1, lret.logr_layout.size());
    EXPECT_EQ(0, lret.logr_layout[0].lo_offset);
    EXPECT_EQ(NFS4_UINT64_MAX, lret.logr_layout[0].lo_length);
    EXPECT_EQ(LAYOUTIOMODE4_READ, lret.logr_layout[0].lo_iomode);

    expectLayoutRecall(lret.logr_stateid);

    // Make a second NfsFilesystem to try to open the file and revoke
    // the layout. We need a second NfsFilesystem so that it has a
    // conflicting clientid
    auto fs2 = make_shared<NfsFilesystem>(
        chan_, client_, clock_, "fs2", idmapper_);

    // Open with oo2 for reading - should not recall the delegation
    OPEN4resok res2;
    std::tie(res2, std::ignore) = open(
        fs2, "foo",
        OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo2);

    EXPECT_EQ(false, recalled_);

    // Upgrade to the open to read/write
    std::tie(res2, std::ignore) = open(
        fs2, "foo",
        OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        OPEN4_SHARE_DENY_NONE,
        oo2);

    EXPECT_EQ(true, recalled_);
    recallThread_.join();

    // Cleanup
    close(fs_, fh, res1.stateid);
    close(fs2, fh, res2.stateid);
}
