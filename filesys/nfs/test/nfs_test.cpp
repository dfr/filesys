#include "filesys/nfs/nfsfs.h"
#include <gmock/gmock.h>

using namespace filesys;
using namespace filesys::nfs;
using namespace std;
using namespace testing;

// Not using gmock for this - this is just to make it easy to trigger timeouts
class MockClock: public detail::Clock
{
public:
    MockClock()
        : now_(chrono::system_clock::now())
    {
    }

    time_point now() override { return now_; }

    template <typename Dur>
    MockClock& operator+=(Dur dur)
    {
        now_ += dur;
        return *this;
    }

private:
    time_point now_;
};

class MockNfs: public INfsProgram3
{
public:
    MOCK_METHOD0(null, void());
    MOCK_METHOD1(getattr, GETATTR3res(const GETATTR3args& _arg0));
    MOCK_METHOD1(setattr, SETATTR3res(const SETATTR3args& _arg0));
    MOCK_METHOD1(lookup, LOOKUP3res(const LOOKUP3args& _arg0));
    MOCK_METHOD1(access, ACCESS3res(const ACCESS3args& _arg0));
    MOCK_METHOD1(readlink, READLINK3res(const READLINK3args& _arg0));
    MOCK_METHOD1(read, READ3res(const READ3args& _arg0));
    MOCK_METHOD1(write, WRITE3res(const WRITE3args& _arg0));
    MOCK_METHOD1(create, CREATE3res(const CREATE3args& _arg0));
    MOCK_METHOD1(mkdir, MKDIR3res(const MKDIR3args& _arg0));
    MOCK_METHOD1(symlink, SYMLINK3res(const SYMLINK3args& _arg0));
    MOCK_METHOD1(mknod, MKNOD3res(const MKNOD3args& _arg0));
    MOCK_METHOD1(remove, REMOVE3res(const REMOVE3args& _arg0));
    MOCK_METHOD1(rmdir, RMDIR3res(const RMDIR3args& _arg0));
    MOCK_METHOD1(rename, RENAME3res(const RENAME3args& _arg0));
    MOCK_METHOD1(link, LINK3res(const LINK3args& _arg0));
    MOCK_METHOD1(readdir, READDIR3res(const READDIR3args& _arg0));
    MOCK_METHOD1(readdirplus, READDIRPLUS3res(const READDIRPLUS3args& _arg0));
    MOCK_METHOD1(fsstat, FSSTAT3res(const FSSTAT3args& _arg0));
    MOCK_METHOD1(fsinfo, FSINFO3res(const FSINFO3args& _arg0));
    MOCK_METHOD1(pathconf, PATHCONF3res(const PATHCONF3args& _arg0));
    MOCK_METHOD1(commit, COMMIT3res(const COMMIT3args& _arg0));
};

class NfsTest: public ::testing::Test
{
public:
    NfsTest()
    {
        proto = make_shared<MockNfs>();
        clock = make_shared<MockClock>();
        nfs = make_shared<NfsFilesystem>(proto, clock, nfs_fh3{{1, 0, 0, 0}});
        ignoreFsinfo();
    }

    static fattr3 fakeAttrs(ftype3 type, fileid3 fileid)
    {
        return fattr3{
            type, 0666, 1, 2, 3, 4, 5, {0, 0}, 6, fileid, {}, {}};
    }

    static GETATTR3res getattrOkResult(ftype3 type, fileid3 fileid)
    {
        return GETATTR3res(NFS3_OK, GETATTR3resok{fakeAttrs(type, fileid)});
    }

    static FSINFO3res fsinfoOkResult()
    {
        return FSINFO3res{
            NFS3_OK,
            FSINFO3resok{
                post_op_attr(false),
                65536,
                65536,
                512,
                65536,
                65536,
                512,
                65536,
                ~0ull,
                {0, 1},
                FSF3_LINK+FSF3_SYMLINK+FSF3_HOMOGENEOUS+FSF3_CANSETTIME}};
    }

    void ignoreGetattr()
    {
        ON_CALL(*proto.get(), getattr(_))
            .WillByDefault(
                InvokeWithoutArgs([=](){ return getattrOkResult(NF3DIR, 1); }));
    }

    void ignoreFsinfo()
    {
        ON_CALL(*proto.get(), fsinfo(_))
            .WillByDefault(
                InvokeWithoutArgs([=](){ return fsinfoOkResult(); }));
    }

    shared_ptr<NfsFilesystem> nfs;
    shared_ptr<MockNfs> proto;
    shared_ptr<MockClock> clock;
};

TEST_F(NfsTest, Root)
{
    // The first call to root() will cause a getattr rpc. Subsequent calls
    // will use the cached value

    EXPECT_CALL(*proto.get(), getattr(_))
        .Times(1)
        .WillOnce(Return(ByMove(getattrOkResult(NF3DIR, 1))));

    nfs->root();
    nfs->root();
}

TEST_F(NfsTest, AttrTimeout)
{
    // The first call to root() will cause a getattr rpc.
    // Calling getattr after a delay will cause another rpc.

    EXPECT_CALL(*proto.get(), getattr(_))
        .Times(2)
        .WillOnce(Return(ByMove(getattrOkResult(NF3DIR, 1))))
        .WillOnce(Return(ByMove(getattrOkResult(NF3DIR, 1))));

    auto root = nfs->root();
    *clock += 2*ATTR_TIMEOUT;
    root->getattr();
}

MATCHER(matchAllSet, "") {
    return arg.new_attributes.mode.set_it &&
        arg.new_attributes.uid.set_it &&
        arg.new_attributes.gid.set_it &&
        arg.new_attributes.size.set_it &&
        arg.new_attributes.atime.set_it == SET_TO_CLIENT_TIME &&
        arg.new_attributes.mtime.set_it == SET_TO_CLIENT_TIME;
}

static auto toNfsTime(chrono::system_clock::time_point time)
{
    using namespace std::chrono;
    auto d = time.time_since_epoch();
    auto sec = duration_cast<seconds>(d);
    auto nsec = duration_cast<nanoseconds>(d) - sec;
    return nfstime3{uint32(sec.count()), uint32(nsec.count())};
}

TEST_F(NfsTest, Setattr)
{
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), setattr(matchAllSet()))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([this]() {
            fattr3 attr = fakeAttrs(NF3DIR, 1);
            // These need to match the values below
            attr.mode = 0777;
            attr.uid = 123;
            attr.gid = 456;
            attr.size = 99;
            attr.atime = toNfsTime(clock->now());
            attr.mtime = toNfsTime(clock->now());
            return SETATTR3res(
                NFS3_OK,
                SETATTR3resok{{
                    pre_op_attr(false),
                    post_op_attr(true, move(attr))}});
        }));

    nfs->root()->setattr([this](auto attrs) {
        attrs->setMode(0777);
        attrs->setUid(123);
        attrs->setGid(456);
        attrs->setSize(99);
        attrs->setAtime(clock->now());
        attrs->setMtime(clock->now());
    });

    // We should not make an RPC if the attribute isn't changed
    nfs->root()->setattr([this](auto attrs) {
        attrs->setSize(99);
    });
}

TEST_F(NfsTest, Lookup1)
{
    // Normal call to lookup returning attributes
    EXPECT_CALL(*proto.get(), getattr(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs(
            []() { return getattrOkResult(NF3DIR, 1); }));

    EXPECT_CALL(*proto.get(), lookup(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return LOOKUP3res(
                NFS3_OK,
                LOOKUP3resok{
                    {{2}},
                    post_op_attr(true, fakeAttrs(NF3REG, 2)),
                    post_op_attr(false)});
        }));

    nfs->root()->lookup("foo");
}

TEST_F(NfsTest, Lookup2)
{
    // Result without attributes
    EXPECT_CALL(*proto.get(), getattr(_))
        .Times(2)
        .WillOnce(InvokeWithoutArgs(
            []() { return getattrOkResult(NF3DIR, 1); }))
        .WillOnce(InvokeWithoutArgs(
            []() { return getattrOkResult(NF3REG, 2); }));

    EXPECT_CALL(*proto.get(), lookup(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return LOOKUP3res(
                NFS3_OK,
                LOOKUP3resok{{}, post_op_attr(false), post_op_attr(false)});
        }));

    nfs->root()->lookup("foo");
}

TEST_F(NfsTest, Lookup3)
{
    // Lookup failure
    EXPECT_CALL(*proto.get(), getattr(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs(
            []() { return getattrOkResult(NF3DIR, 1); }));

    EXPECT_CALL(*proto.get(), lookup(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return LOOKUP3res(
                NFS3ERR_NOENT,
                LOOKUP3resfail{post_op_attr(false)});
        }));

    EXPECT_THROW(nfs->root()->lookup("foo"), system_error);
}

TEST_F(NfsTest, Open)
{
    // If we don't specify CREATE, open should call lookup
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), lookup(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return LOOKUP3res(
                NFS3_OK,
                LOOKUP3resok{
                    {{2}},
                    post_op_attr(true, fakeAttrs(NF3REG, 2)),
                    post_op_attr(false)});
        }));

    nfs->root()->open("foo", 0, [](auto){});
}

MATCHER(matchUnchecked, "") {
    return arg.how.mode == UNCHECKED;
}

TEST_F(NfsTest, Create)
{
    // If we specify CREATE, open should call create with mode UNCHECKED
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), create(matchUnchecked()))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return CREATE3res(
                NFS3_OK,
                CREATE3resok{
                    post_op_fh3(true, {{2}}),
                    post_op_attr(true, fakeAttrs(NF3REG, 2)),
                    {pre_op_attr(false), post_op_attr(false)}});
        }));

    nfs->root()->open("foo", OpenFlags::CREATE, [](auto){});

    // Check an exception is raised on failure
    EXPECT_CALL(*proto.get(), create(matchUnchecked()))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return CREATE3res(
                NFS3ERR_INVAL,
                CREATE3resfail{
                    {pre_op_attr(false), post_op_attr(false)}});
        }));

    EXPECT_THROW(
        nfs->root()->open("foo", OpenFlags::CREATE, [](auto){}),
        system_error);

}

MATCHER(matchGuarded, "") {
    return arg.how.mode == GUARDED;
}

TEST_F(NfsTest, CreateExclusive)
{
    // If we specify CREATE+EXCLUSIVE, open should call create with mode
    // GUARDED
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), create(matchGuarded()))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return CREATE3res(
                NFS3_OK,
                CREATE3resok{
                    post_op_fh3(true, {{2}}),
                    post_op_attr(true, fakeAttrs(NF3REG, 2)),
                    {pre_op_attr(false), post_op_attr(false)}});
        }));

    nfs->root()->open(
        "foo", OpenFlags::CREATE+OpenFlags::EXCLUSIVE, [](auto){});
}

TEST_F(NfsTest, CreateTruncate)
{
    // If we specify CREATE+TRUNCATE, open should call create with mode
    // UNCHECKED. If the returned attributes have a non-zero size, it
    // should call setattr.
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), create(matchUnchecked()))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return CREATE3res(
                NFS3_OK,
                CREATE3resok{
                    post_op_fh3(true, {{2}}),
                    post_op_attr(true, fakeAttrs(NF3REG, 2)),
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    EXPECT_CALL(*proto.get(), setattr(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            fattr3 attrs = fakeAttrs(NF3REG, 2);
            attrs.size = 0;
            return SETATTR3res(
                NFS3_OK,
                SETATTR3resok{{
                    pre_op_attr(false),
                    post_op_attr(true, move(attrs))}});
        }));

    auto f = nfs->root()->open(
        "foo", OpenFlags::CREATE+OpenFlags::TRUNCATE, [](auto){});
    EXPECT_EQ(0, f->getattr()->size());

    // Make sure an exception is raised if the setattr fails
    EXPECT_CALL(*proto.get(), create(matchUnchecked()))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return CREATE3res(
                NFS3_OK,
                CREATE3resok{
                    post_op_fh3(true, {{3}}),
                    post_op_attr(true, fakeAttrs(NF3REG, 3)),
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    EXPECT_CALL(*proto.get(), setattr(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return SETATTR3res(
                NFS3ERR_INVAL,
                SETATTR3resfail{{pre_op_attr(false), post_op_attr(false)}});
        }));

    EXPECT_THROW(
        nfs->root()->open(
            "foo", OpenFlags::CREATE+OpenFlags::TRUNCATE, [](auto){}),
        system_error);
}

TEST_F(NfsTest, CreateTruncate2)
{
    // If the call to create doesn't return a filehandle and attributes,
    // verify that lookup is called.
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), create(matchUnchecked()))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return CREATE3res(
                NFS3_OK,
                CREATE3resok{
                    post_op_fh3(false),
                    post_op_attr(false),
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    EXPECT_CALL(*proto.get(), lookup(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return LOOKUP3res(
                NFS3_OK,
                LOOKUP3resok{{{2}},
                post_op_attr(true, fakeAttrs(NF3REG, 2)),
                post_op_attr(false)});
        }));
    EXPECT_CALL(*proto.get(), setattr(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            fattr3 attrs = fakeAttrs(NF3REG, 2);
            attrs.size = 0;
            return SETATTR3res(
                NFS3_OK,
                SETATTR3resok{{
                    pre_op_attr(false),
                    post_op_attr(true, move(attrs))}});
        }));

    auto f = nfs->root()->open(
        "foo", OpenFlags::CREATE+OpenFlags::TRUNCATE, [](auto){});
    EXPECT_EQ(0, f->getattr()->size());
}

TEST_F(NfsTest, Commit)
{
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), commit(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return COMMIT3res(
                NFS3_OK,
                COMMIT3resok{
                    {pre_op_attr(false), post_op_attr(false)}, {}});
        }));
    nfs->root()->commit();

    EXPECT_CALL(*proto.get(), commit(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return COMMIT3res(
                NFS3ERR_INVAL,
                COMMIT3resfail{
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    EXPECT_THROW(nfs->root()->commit(), system_error);
}

TEST_F(NfsTest, Readlink)
{
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), readlink(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return READLINK3res(
                NFS3_OK,
                READLINK3resok{
                    post_op_attr(false), "link value"});
        }));
    EXPECT_EQ("link value", nfs->root()->readlink());

    EXPECT_CALL(*proto.get(), readlink(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return READLINK3res(
                NFS3ERR_INVAL,
                READLINK3resfail{post_op_attr(false)});
        }));
    EXPECT_THROW(nfs->root()->readlink(), system_error);
}

TEST_F(NfsTest, Read)
{
    bool atEof;

    ignoreGetattr();

    EXPECT_CALL(*proto.get(), read(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            auto buf = make_shared<oncrpc::Buffer>(1024);
            fill_n(buf->data(), 1024, 99);
            return READ3res(
                NFS3_OK,
                READ3resok{
                    post_op_attr(false),
                    1024,
                    false,
                    buf});
        }));

    auto buf = nfs->root()->read(0, 1024, atEof);
    EXPECT_EQ(1024, buf->size());
    for (auto v: *buf)
        EXPECT_EQ(99, v);

    EXPECT_CALL(*proto.get(), read(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return READ3res(
                NFS3ERR_INVAL,
                READ3resfail{post_op_attr(false)});
        }));
    EXPECT_THROW(nfs->root()->read(1024, 1024, atEof), system_error);
}

TEST_F(NfsTest, Write)
{
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), write(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return WRITE3res(
                NFS3_OK,
                WRITE3resok{
                    {pre_op_attr(false), post_op_attr(false)},
                    123, DATA_SYNC, {}});
        }));
    auto buf = make_shared<oncrpc::Buffer>(1024);
    fill_n(buf->data(), 1024, 99);
    EXPECT_EQ(123, nfs->root()->write(0, buf));

    EXPECT_CALL(*proto.get(), write(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return WRITE3res(
                NFS3ERR_INVAL,
                WRITE3resfail{
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    EXPECT_THROW(nfs->root()->write(1024, buf), system_error);
}

TEST_F(NfsTest, Mkdir)
{
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), mkdir(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return MKDIR3res(
                NFS3_OK,
                MKDIR3resok{
                    post_op_fh3(true, {{2}}),
                    post_op_attr(true, fakeAttrs(NF3DIR, 2)),
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    nfs->root()->mkdir("foo", [](auto){});

    EXPECT_CALL(*proto.get(), mkdir(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return MKDIR3res(
                NFS3ERR_INVAL,
                MKDIR3resfail{
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    EXPECT_THROW(nfs->root()->mkdir("bar", [](auto){}), system_error);
}

TEST_F(NfsTest, Symlink)
{
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), symlink(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return SYMLINK3res(
                NFS3_OK,
                SYMLINK3resok{
                    post_op_fh3(true, {{2}}),
                    post_op_attr(true, fakeAttrs(NF3LNK, 2)),
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    nfs->root()->symlink("foo", "data", [](auto){});

    EXPECT_CALL(*proto.get(), symlink(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return SYMLINK3res(
                NFS3ERR_INVAL,
                SYMLINK3resfail{
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    EXPECT_THROW(nfs->root()->symlink("bar", "data", [](auto){}), system_error);
}

TEST_F(NfsTest, Mkfifo)
{
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), mknod(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return MKNOD3res(
                NFS3_OK,
                MKNOD3resok{
                    post_op_fh3(true, {{2}}),
                    post_op_attr(true, fakeAttrs(NF3LNK, 2)),
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    nfs->root()->mkfifo("foo", [](auto){});

    EXPECT_CALL(*proto.get(), mknod(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return MKNOD3res(
                NFS3ERR_INVAL,
                MKNOD3resfail{
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    EXPECT_THROW(nfs->root()->mkfifo("bar", [](auto){}), system_error);
}

TEST_F(NfsTest, Remove)
{
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), remove(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return REMOVE3res(
                NFS3_OK,
                REMOVE3resok{
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    nfs->root()->remove("foo");

    EXPECT_CALL(*proto.get(), remove(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return REMOVE3res(
                NFS3ERR_INVAL,
                REMOVE3resfail{
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    EXPECT_THROW(nfs->root()->remove("bar"), system_error);
}

TEST_F(NfsTest, Rmdir)
{
    ignoreGetattr();

    EXPECT_CALL(*proto.get(), rmdir(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return RMDIR3res(
                NFS3_OK,
                RMDIR3resok{
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    nfs->root()->rmdir("foo");

    EXPECT_CALL(*proto.get(), rmdir(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return RMDIR3res(
                NFS3ERR_INVAL,
                RMDIR3resfail{
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    EXPECT_THROW(nfs->root()->rmdir("bar"), system_error);
}

TEST_F(NfsTest, Rename)
{
    ignoreGetattr();
    auto dir = nfs->root();

    EXPECT_CALL(*proto.get(), rename(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return RENAME3res(
                NFS3_OK,
                RENAME3resok{
                    {pre_op_attr(false), post_op_attr(false)},
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    dir->rename("foo", dir, "bar");

    EXPECT_CALL(*proto.get(), rename(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return RENAME3res(
                NFS3ERR_INVAL,
                RENAME3resfail{
                    {pre_op_attr(false), post_op_attr(false)},
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    EXPECT_THROW(dir->rename("bar", dir, "foo"), system_error);
}

TEST_F(NfsTest, Link)
{
    ignoreGetattr();
    auto dir = nfs->root();

    EXPECT_CALL(*proto.get(), link(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return LINK3res(
                NFS3_OK,
                LINK3resok{
                    post_op_attr(false),
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    dir->link("foo", dir);

    EXPECT_CALL(*proto.get(), link(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return LINK3res(
                NFS3ERR_INVAL,
                LINK3resfail{
                    post_op_attr(false),
                    {pre_op_attr(false), post_op_attr(false)}});
        }));
    EXPECT_THROW(dir->link("bar", dir), system_error);
}

TEST_F(NfsTest, Readdir)
{
    ignoreGetattr();
    auto dir = nfs->root();

    EXPECT_CALL(*proto.get(), readdirplus(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            dirlistplus3 dirlist;
            int id = 1;
            for (auto name: {"foo", "bar", "baz"}) {
                ++id;
                auto p = make_unique<entryplus3>();
                p->fileid = id;
                p->name = name;
                p->cookie = id;
                p->name_attributes = post_op_attr(false),
                p->name_handle = post_op_fh3(false),
                p->nextentry = move(dirlist.entries);
                dirlist.entries = move(p);
            }
            dirlist.eof = true;
            return READDIRPLUS3res(
                NFS3_OK,
                READDIRPLUS3resok{
                    post_op_attr(false),
                    {},
                    move(dirlist)});
        }));
    auto iter = dir->readdir(0);
    EXPECT_EQ("baz", iter->name()); iter->next();
    EXPECT_EQ("bar", iter->name()); iter->next();
    EXPECT_EQ("foo", iter->name()); iter->next();
    EXPECT_EQ(false, iter->valid());

    EXPECT_CALL(*proto.get(), readdirplus(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs([]() {
            return READDIRPLUS3res(
                NFS3ERR_INVAL,
                READDIRPLUS3resfail{post_op_attr(false)});
        }));
    EXPECT_THROW(dir->readdir(0), system_error);
}

TEST_F(NfsTest, Readdir2)
{
    ignoreFsinfo();

    // Verify that readdir uses the returned file attributes - we should
    // see exactly one call to getattr() for the root directory

    EXPECT_CALL(*proto.get(), getattr(_))
        .Times(1)
        .WillOnce(InvokeWithoutArgs(
            []() { return getattrOkResult(NF3DIR, 1); }));

    EXPECT_CALL(*proto.get(), readdirplus(_))
        .Times(2)
        .WillRepeatedly(InvokeWithoutArgs([]() {
            dirlistplus3 dirlist;
            int id = 1;
            for (auto name: {"foo", "bar", "baz"}) {
                ++id;
                auto p = make_unique<entryplus3>();
                p->fileid = id;
                p->name = name;
                p->cookie = id;
                p->name_attributes = post_op_attr(true, fakeAttrs(NF3REG, id)),
                p->name_handle = post_op_fh3(
                    true, nfs_fh3{{uint8_t(id), 0, 0, 0}}),
                p->nextentry = move(dirlist.entries);
                dirlist.entries = move(p);
            }
            dirlist.eof = true;
            return READDIRPLUS3res(
                NFS3_OK,
                READDIRPLUS3resok{
                    post_op_attr(false),
                    {},
                    move(dirlist)});
        }));

    auto dir = nfs->root();

    // The first readdir will create the local file nodes with attributes
    // and file handles returned by readdirplus
    auto iter = dir->readdir(0);
    EXPECT_EQ("baz", iter->name());
    EXPECT_EQ(FileType::FILE, iter->file()->getattr()->type());
    iter->next();
    EXPECT_EQ("bar", iter->name());
    EXPECT_EQ(FileType::FILE, iter->file()->getattr()->type());
    iter->next();
    EXPECT_EQ("foo", iter->name());
    EXPECT_EQ(FileType::FILE, iter->file()->getattr()->type());
    iter->next();
    EXPECT_EQ(false, iter->valid());

    // Advance the clock to invalidate the cached attributes
    *clock += 2*ATTR_TIMEOUT;

    // The second readdir should refresh the attributes - we should not see
    // any subsequent calls to getattr
    iter = dir->readdir(0);
    EXPECT_EQ("baz", iter->name());
    EXPECT_EQ(FileType::FILE, iter->file()->getattr()->type());
    iter->next();
    EXPECT_EQ("bar", iter->name());
    EXPECT_EQ(FileType::FILE, iter->file()->getattr()->type());
    iter->next();
    EXPECT_EQ("foo", iter->name());
    EXPECT_EQ(FileType::FILE, iter->file()->getattr()->type());
    iter->next();
    EXPECT_EQ(false, iter->valid());
}
