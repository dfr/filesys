#include <gtest/gtest.h>

#include <fs++/filesys.h>
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

class Nfs3Test: public ::testing::Test
{
public:
    Nfs3Test()
        : fsman_(FilesystemManager::instance())
    {
        // Create a scratch filesystem to 'export'
        system("rm -rf ./testdb");
        objfs_ = fsman_.mount<ObjFilesystem>("/", "testdb");

        // Register mount and nfs services with oncrpc
        svcreg_ = make_shared<ServiceRegistry>();
        chan_ = make_shared<LocalChannel>(svcreg_);
        nfsd::nfs3::init(svcreg_);

        // Try to mount our test filesystem
        Mountprog3<> prog(chan_);
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
        auto proto = make_shared<NfsProgram3<oncrpc::SysClient>>(chan_);
        auto clock = make_shared<detail::SystemClock>();
        fs_ = make_shared<NfsFilesystem>(
            proto, clock, nfs_fh3{move(info.fhandle)});
    }

    ~Nfs3Test()
    {
        fsman_.unmountAll();
    }

    FilesystemManager& fsman_;
    shared_ptr<ObjFilesystem> objfs_;       // objfs backing store
    shared_ptr<NfsFilesystem> fs_;          // nfsfs interface to objfs
    shared_ptr<ServiceRegistry> svcreg_;    // oncrpc plumbing
    shared_ptr<Channel> chan_;              // oncrpc plumbing
};

TEST_F(Nfs3Test, Init)
{
    auto root = fs_->root();
    EXPECT_EQ(1, int(root->getattr()->fileid()));
    EXPECT_EQ(2, root->getattr()->nlink());
    EXPECT_EQ(root, root->lookup("."));
    EXPECT_EQ(root, root->lookup(".."));
}

TEST_F(Nfs3Test, Open)
{
    auto root = fs_->root();
    EXPECT_THROW(root->open("foo", 0, [](auto){}), system_error);
    auto file = root->open(
        "foo", OpenFlags::RDWR+OpenFlags::CREATE,
        [](auto attr){ attr->setMode(0666); });
    EXPECT_EQ(0666, file->getattr()->mode());
    EXPECT_EQ(file, root->open("foo", OpenFlags::RDWR, [](auto){}));
    EXPECT_THROW(root->open(
        "foo", OpenFlags::RDWR+OpenFlags::CREATE+OpenFlags::EXCLUSIVE,
        [](auto){}),
        system_error);
}

TEST_F(Nfs3Test, ReadWrite)
{
    auto root = fs_->root();
    auto blockSize = objfs_->blockSize();
    auto file = root->open(
        "foo", OpenFlags::RDWR+OpenFlags::CREATE,
        [](auto attr){ attr->setMode(0666); });
    uint8_t buf[] = {'f', 'o', 'o'};

    // Write at start
    file->write(0, make_shared<oncrpc::Buffer>(3, buf));
    EXPECT_EQ(3, file->getattr()->size());
    bool eof;
    EXPECT_EQ("foo", file->read(0, 3, eof)->toString());
    EXPECT_EQ(true, eof);

    // Extend by one block, writing over the block boundary
    file->write(blockSize - 1, make_shared<oncrpc::Buffer>(3, buf));
    EXPECT_EQ(blockSize + 2, file->getattr()->size());
    EXPECT_EQ("foo", file->read(4095, 3, eof)->toString());
    EXPECT_EQ(true, eof);

    // Extend with a hole
    file->write(4*blockSize, make_shared<oncrpc::Buffer>(3, buf));
    EXPECT_EQ(4*blockSize + 3, file->getattr()->size());
    EXPECT_EQ("foo", file->read(4*blockSize, 3, eof)->toString());
    EXPECT_EQ(true, eof);

    // Make sure the hole reads as zero
    auto block = file->read(3*blockSize, blockSize, eof);
    EXPECT_EQ(blockSize, block->size());
    for (int i = 0; i < blockSize; i++)
        EXPECT_EQ(0, block->data()[i]);
    EXPECT_EQ(false, eof);
}

TEST_F(Nfs3Test, Truncate)
{
    auto root = fs_->root();
    auto blockSize = objfs_->blockSize();
    auto file = root->open(
        "foo", OpenFlags::RDWR+OpenFlags::CREATE,
        [](auto attr){ attr->setMode(0666); });
    auto buf = make_shared<oncrpc::Buffer>(blockSize);
    fill_n(buf->data(), blockSize, 1);
    for (int i = 0; i < 10; i++)
        file->write(i * blockSize, buf);
    EXPECT_GE(10 * blockSize, file->getattr()->used());
    file->setattr([](auto attr){ attr->setSize(0); });
    file->setattr([=](auto attr){ attr->setSize(blockSize); });
    bool eof;
    EXPECT_EQ(0, file->read(0, 1, eof)->data()[0]);
}

TEST_F(Nfs3Test, Mtime)
{
    auto root = fs_->root();
    auto blockSize = objfs_->blockSize();
    auto file = root->open(
        "foo", OpenFlags::RDWR+OpenFlags::CREATE,
        [](auto attr){ attr->setMode(0666); });
    auto buf = make_shared<oncrpc::Buffer>(blockSize);
    fill_n(buf->data(), blockSize, 1);
    auto mtime = file->getattr()->atime();
    file->write(0, buf);
    EXPECT_LT(mtime, file->getattr()->mtime());
}

TEST_F(Nfs3Test, Atime)
{
    auto root = fs_->root();
    auto blockSize = objfs_->blockSize();
    auto file = root->open(
        "foo", OpenFlags::RDWR+OpenFlags::CREATE,
        [](auto attr){ attr->setMode(0666); });
    auto buf = make_shared<oncrpc::Buffer>(blockSize);
    fill_n(buf->data(), blockSize, 1);
    file->write(0, buf);
    auto atime = file->getattr()->atime();
    bool eof;
    file->read(0, blockSize, eof);
    EXPECT_LT(atime, file->getattr()->atime());
}

TEST_F(Nfs3Test, Symlink)
{
    auto root = fs_->root();
    auto link = root->symlink("foo", "bar", [](auto){});
    EXPECT_EQ(1, link->getattr()->nlink());
    EXPECT_EQ(FileType::SYMLINK, link->getattr()->type());
    EXPECT_EQ(3, link->getattr()->size());
    EXPECT_EQ("bar", link->readlink());
    EXPECT_EQ(3, root->getattr()->size());
}

TEST_F(Nfs3Test, Mkfifo)
{
    auto root = fs_->root();
    auto fifo = root->mkfifo("foo", [](auto){});
    EXPECT_EQ(1, fifo->getattr()->nlink());
    EXPECT_EQ(FileType::FIFO, fifo->getattr()->type());
    EXPECT_EQ(3, root->getattr()->size());
}

TEST_F(Nfs3Test, Mkdir)
{
    auto root = fs_->root();
    auto dir = root->mkdir("foo", [](auto attr){ attr->setMode(0700); });
    EXPECT_EQ(2, dir->getattr()->nlink());
    EXPECT_EQ(0700, dir->getattr()->mode());
    EXPECT_EQ(3, root->getattr()->nlink());
    EXPECT_EQ(dir, dir->lookup("."));
    EXPECT_EQ(root, dir->lookup(".."));
    EXPECT_EQ(3, root->getattr()->size());
}

TEST_F(Nfs3Test, Remove)
{
    auto root = fs_->root();
    auto link = root->symlink("foo", "bar", [](auto){});
    auto dir = root->mkdir("baz", [](auto){});
    EXPECT_THROW(root->remove("baz"), system_error);
    root->remove("foo");
    EXPECT_EQ(3, root->getattr()->size());
}

TEST_F(Nfs3Test, Rmdir)
{
    auto root = fs_->root();
    auto dir = root->mkdir("foo", [](auto){});
    root->rmdir("foo");
    EXPECT_EQ(2, int(root->getattr()->nlink()));
    EXPECT_EQ(2, root->getattr()->size());
    EXPECT_THROW(root->lookup("foo"), system_error);

    root->symlink("foo", "bar", [](auto){});
    EXPECT_THROW(root->rmdir("foo"), system_error);
}

TEST_F(Nfs3Test, RenameFile)
{
    auto root = fs_->root();
    auto file = root->mkfifo("foo", [](auto){});

    // Simple rename within root directory
    root->rename("bar", root, "foo");
    EXPECT_THROW(root->lookup("foo"), system_error);
    EXPECT_EQ(file, root->lookup("bar"));
    EXPECT_EQ(3, root->getattr()->size());

    // Rename to a different directory
    auto dir = root->mkdir("foo", [](auto){});
    EXPECT_EQ(4, root->getattr()->size());
    dir->rename("bar", root, "bar");
    EXPECT_THROW(root->lookup("bar"), system_error);
    EXPECT_EQ(file, dir->lookup("bar"));
    EXPECT_EQ(3, root->getattr()->size());
    EXPECT_EQ(3, dir->getattr()->size());

    // Renaming to an existing name should delete the target first ...
    auto file2 = dir->mkfifo("foo", [](auto){});
    EXPECT_EQ(4, dir->getattr()->size());
    dir->rename("foo", dir, "bar");
    EXPECT_EQ(file, dir->lookup("foo"));
    EXPECT_EQ(3, dir->getattr()->size());

    // ... unless the target is a non-empty directory
    dir->mkdir("bar", [](auto){})->mkfifo("aaa", [](auto){});
    EXPECT_EQ(4, dir->getattr()->size());
    EXPECT_THROW(dir->rename("bar", dir, "foo"), system_error);

    // Check that the rename succeeds when the target is empty and verify
    // the size and link count
    dir->lookup("bar")->remove("aaa");
    EXPECT_EQ(3, dir->getattr()->nlink());
    dir->rename("bar", dir, "foo");
    EXPECT_EQ(2, dir->getattr()->nlink());
    EXPECT_EQ(3, dir->getattr()->size());
}

TEST_F(Nfs3Test, RenameDir)
{
    auto root = fs_->root();
    auto dir = root->mkdir("foo", [](auto){});

    // Simple rename within root directory
    root->rename("bar", root, "foo");
    EXPECT_THROW(root->lookup("foo"), system_error);
    EXPECT_EQ(dir, root->lookup("bar"));

    // Rename to a different directory
    auto dir2 = root->mkdir("foo", [](auto){});
    EXPECT_EQ(4, root->getattr()->nlink());
    EXPECT_EQ(2, dir->getattr()->nlink());
    dir->rename("foo", root, "foo");
    EXPECT_EQ(3, root->getattr()->nlink());
    EXPECT_EQ(3, dir->getattr()->nlink());
    EXPECT_THROW(root->lookup("foo"), system_error);
    EXPECT_EQ(dir2, dir->lookup("foo"));
    EXPECT_EQ(dir, dir2->lookup(".."));
}

TEST_F(Nfs3Test, Link)
{
    auto root = fs_->root();
    auto a = root->mkdir("a", [](auto){});
    auto b = root->mkfifo("b", [](auto){});
    EXPECT_THROW(root->link("aa", a), system_error);
    root->link("bb", b);
    EXPECT_EQ(b, root->lookup("bb"));
}

TEST_F(Nfs3Test, Readdir)
{
    auto root = fs_->root();
    root->mkdir("foo", [](auto){});
    root->mkdir("bar", [](auto){});
    root->mkdir("baz", [](auto){});
    set<string> names;
    for (auto iter = root->readdir(0); iter->valid(); iter->next()) {
        names.insert(iter->name());
    }
    EXPECT_EQ(5, names.size());
    EXPECT_NE(names.end(), names.find("."));
    EXPECT_NE(names.end(), names.find(".."));
    EXPECT_NE(names.end(), names.find("foo"));
    EXPECT_NE(names.end(), names.find("bar"));
    EXPECT_NE(names.end(), names.find("baz"));
}

TEST_F(Nfs3Test, ReaddirLarge)
{
    auto root = fs_->root();
    for (int i = 0; i < 1000; i++)
        root->symlink(to_string(i), "foo", [](auto){});
    int count = 0;
    for (auto iter = root->readdir(0); iter->valid(); iter->next()) {
        count++;
    }
    EXPECT_EQ(1002, count);
}
